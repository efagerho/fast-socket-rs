#[path = "../common.rs"]
mod common;

use std::net::SocketAddrV4;
use std::sync::Arc;

use clap::Parser;
use common::{
    Backend, BoxError, DEFAULT_BATCH_SIZE, DEFAULT_THREADS, install_shutdown_signal_handlers,
    normalize_batch_size, normalize_payload_len, normalize_xdp_bind, payload,
};
use fast_socket_os_rs::OsUdpSocket;
use fast_socket_rs::{
    PacketBufferMut, RecvBatch, TxSlot, UdpReceive, UdpRecvMeta, UdpRxBuffer,
    UdpSocket as FastUdpSocket, UdpTransmit, UdpTxBuffer, UdpTxBufferMut,
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
    #[arg(long, default_value_t = DEFAULT_THREADS)]
    threads: usize,
    #[arg(long, default_value_t = DEFAULT_BATCH_SIZE)]
    batch_size: usize,
    #[arg(long, default_value_t = 64)]
    response_len: usize,
}

fn main() -> Result<(), BoxError> {
    install_shutdown_signal_handlers()?;
    let args = Args::parse();
    let batch_size = normalize_batch_size(args.batch_size)?;
    let response_len = normalize_payload_len(args.response_len)?;
    let response = Arc::new(payload(response_len));

    match args.backend {
        Backend::Os => common::run_os_socket_loop(
            "udp-pong os",
            &args.device,
            args.bind,
            batch_size,
            response_len,
            PongState::<OsUdpSocket>::new(batch_size, Arc::clone(&response)),
            pong_step::<OsUdpSocket>,
        ),
        Backend::Xdp => {
            let bind = normalize_xdp_bind(&args.device, args.bind)?;
            common::run_xdp_busy_poll_loop(
                "udp-pong xdp",
                &args.device,
                bind,
                args.threads,
                move || PongState::<BusyPollXdpUdpSocket>::new(batch_size, Arc::clone(&response)),
                pong_step::<BusyPollXdpUdpSocket>,
            )
        }
    }
}

struct PongState<S: FastUdpSocket<RecvMeta = UdpRecvMeta>> {
    rx: RecvBatch<UdpReceive<UdpRxBuffer<S>, UdpRecvMeta>>,
    tx_buffers: Vec<UdpTxBufferMut<S>>,
    tx: Vec<TxSlot<UdpTransmit<UdpTxBuffer<S>>>>,
    response: Arc<Vec<u8>>,
}

impl<S> PongState<S>
where
    S: FastUdpSocket<RecvMeta = UdpRecvMeta>,
{
    fn new(batch_size: usize, response: Arc<Vec<u8>>) -> Self {
        Self {
            rx: RecvBatch::with_capacity(batch_size),
            tx_buffers: Vec::with_capacity(batch_size),
            tx: Vec::with_capacity(batch_size),
            response,
        }
    }
}

fn pong_step<S>(socket: &mut S, state: &mut PongState<S>) -> Result<usize, BoxError>
where
    S: FastUdpSocket<RecvMeta = UdpRecvMeta>,
{
    state.rx.clear();
    let received = socket.recv(&mut state.rx)?;
    if received == 0 {
        return Ok(0);
    }

    state.tx_buffers.clear();
    state.tx.clear();
    socket.allocate_tx_batch(&mut state.tx_buffers, received)?;

    for item in state.rx.drain() {
        let Some(mut buffer) = state.tx_buffers.pop() else {
            break;
        };
        buffer.extend_from_slice(&state.response)?;
        let mut tx = UdpTransmit::new(buffer.freeze(), item.meta.source);
        tx.source_port = item.meta.destination_port;
        state.tx.push(TxSlot::Ready(tx));
    }

    let sent = common::send_all(socket, &mut state.tx)?;
    socket.drain_tx_completions()?;
    Ok(sent)
}
