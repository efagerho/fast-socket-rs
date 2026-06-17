use fast_socket_examples as common;

use std::net::{SocketAddr, SocketAddrV4};

use clap::Parser;
use common::{
    Backend, BoxError, DEFAULT_BATCH_SIZE, DEFAULT_PAYLOAD_CAPACITY, DEFAULT_THREADS,
    install_shutdown_signal_handlers, normalize_batch_size, normalize_xdp_bind,
};
use fast_socket_os_rs::OsUdpSocket;
use fast_socket_rs::{
    PacketBufferMut, RecvBatch, TxSlot, UdpReceive, UdpRecvMeta, UdpRxBuffer,
    UdpSocket as FastUdpSocket, UdpTransmit, UdpTxBuffer,
};
use fast_socket_xdp_rs::BusyPollXdpUdpSocket;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, value_enum, ignore_case = true)]
    backend: Backend,
    #[arg(long)]
    device: String,
    #[arg(long)]
    bind: SocketAddrV4,
    #[arg(long)]
    upstream: SocketAddrV4,
    #[arg(long, default_value_t = DEFAULT_THREADS)]
    threads: usize,
    #[arg(long, default_value_t = DEFAULT_BATCH_SIZE)]
    batch_size: usize,
    #[arg(long, default_value_t = DEFAULT_PAYLOAD_CAPACITY)]
    payload_capacity: usize,
}

fn main() -> Result<(), BoxError> {
    install_shutdown_signal_handlers()?;
    let args = Args::parse();
    let batch_size = normalize_batch_size(args.batch_size)?;
    let upstream: SocketAddr = args.upstream.into();

    match args.backend {
        Backend::Os => common::run_os_socket_loop(
            "udp-proxy os",
            &args.device,
            args.bind,
            batch_size,
            args.payload_capacity,
            ProxyState::<OsUdpSocket>::new(batch_size, upstream),
            proxy_step::<OsUdpSocket>,
        ),
        Backend::Xdp => {
            let bind = normalize_xdp_bind(&args.device, args.bind)?;
            common::run_xdp_busy_poll_loop(
                "udp-proxy xdp",
                &args.device,
                bind,
                args.threads,
                move || ProxyState::<BusyPollXdpUdpSocket>::new(batch_size, upstream),
                proxy_step::<BusyPollXdpUdpSocket>,
            )
        }
    }
}

struct ProxyState<S: FastUdpSocket<RecvMeta = UdpRecvMeta>> {
    rx: RecvBatch<UdpReceive<UdpRxBuffer<S>, UdpRecvMeta>>,
    tx: Vec<TxSlot<UdpTransmit<UdpTxBuffer<S>>>>,
    upstream: SocketAddr,
    last_client: Option<SocketAddr>,
}

impl<S> ProxyState<S>
where
    S: FastUdpSocket<RecvMeta = UdpRecvMeta>,
{
    fn new(batch_size: usize, upstream: SocketAddr) -> Self {
        Self {
            rx: RecvBatch::with_capacity(batch_size),
            tx: Vec::with_capacity(batch_size),
            upstream,
            last_client: None,
        }
    }
}

fn proxy_step<S>(socket: &mut S, state: &mut ProxyState<S>) -> Result<usize, BoxError>
where
    S: FastUdpSocket<RecvMeta = UdpRecvMeta>,
    UdpRxBuffer<S>: PacketBufferMut<Frozen = UdpTxBuffer<S>>,
{
    state.rx.clear();
    if socket.recv(&mut state.rx)? == 0 {
        return Ok(0);
    }

    state.tx.clear();
    for item in state.rx.drain() {
        let destination = if item.meta.source == state.upstream {
            let Some(client) = state.last_client else {
                continue;
            };
            client
        } else {
            state.last_client = Some(item.meta.source);
            state.upstream
        };

        let mut tx = UdpTransmit::new(item.packet.freeze(), destination);
        tx.source_port = item.meta.destination_port;
        state.tx.push(TxSlot::Ready(tx));
    }

    let sent = socket.send_all(&mut state.tx)?;
    socket.notify_tx()?;
    socket.drain_tx_completions()?;
    Ok(sent)
}
