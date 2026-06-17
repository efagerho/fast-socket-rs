use fast_socket_examples as common;

use std::net::SocketAddrV4;

use clap::Parser;
use common::{
    Backend, BoxError, DEFAULT_BATCH_SIZE, DEFAULT_PAYLOAD_CAPACITY, DEFAULT_THREADS,
    install_shutdown_signal_handlers, normalize_batch_size, normalize_xdp_bind,
};
use fast_socket_os_rs::OsUdpSocket;
use fast_socket_rs::{RecvBatch, UdpReceive, UdpRecvMeta, UdpRxBuffer, UdpSocket as FastUdpSocket};
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
    #[arg(long, default_value_t = DEFAULT_PAYLOAD_CAPACITY)]
    payload_capacity: usize,
}

fn main() -> Result<(), BoxError> {
    install_shutdown_signal_handlers()?;
    let args = Args::parse();
    let batch_size = normalize_batch_size(args.batch_size)?;

    match args.backend {
        Backend::Os => common::run_os_socket_loop(
            "udp-discard os",
            &args.device,
            args.bind,
            batch_size,
            args.payload_capacity,
            DiscardState::<OsUdpSocket>::new(batch_size),
            discard_step::<OsUdpSocket>,
        ),
        Backend::Xdp => {
            let bind = normalize_xdp_bind(&args.device, args.bind)?;
            common::run_xdp_busy_poll_loop(
                "udp-discard xdp",
                &args.device,
                bind,
                args.threads,
                move || DiscardState::<BusyPollXdpUdpSocket>::new(batch_size),
                discard_step::<BusyPollXdpUdpSocket>,
            )
        }
    }
}

struct DiscardState<S: FastUdpSocket<RecvMeta = UdpRecvMeta>> {
    rx: RecvBatch<UdpReceive<UdpRxBuffer<S>, UdpRecvMeta>>,
}

impl<S> DiscardState<S>
where
    S: FastUdpSocket<RecvMeta = UdpRecvMeta>,
{
    fn new(batch_size: usize) -> Self {
        Self {
            rx: RecvBatch::with_capacity(batch_size),
        }
    }
}

fn discard_step<S>(socket: &mut S, state: &mut DiscardState<S>) -> Result<usize, BoxError>
where
    S: FastUdpSocket<RecvMeta = UdpRecvMeta>,
{
    state.rx.clear();
    let received = socket.recv(&mut state.rx)?;
    state.rx.clear();
    socket.drain_tx_completions()?;
    Ok(received)
}
