#[path = "../common.rs"]
mod common;

use std::net::SocketAddrV4;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use clap::Parser;
use common::{
    Backend, BoxError, DEFAULT_BATCH_SIZE, DEFAULT_PAYLOAD_CAPACITY, DEFAULT_THREADS,
    install_shutdown_signal_handlers, normalize_batch_size, normalize_xdp_bind,
};
use fast_socket_async_rs::{ActorTxPacket, AsyncUdpHandle, AsyncUdpRx};
use fast_socket_os_rs::OsUdpSocket;
use fast_socket_rs::{
    PacketBufferMut, UdpRecvMeta, UdpRxBuffer, UdpSocket as FastUdpSocket, UdpTxBuffer,
    WaitDrivenDriverKind,
};
use fast_socket_xdp_rs::WaitDrivenXdpUdpSocket;

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

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    install_shutdown_signal_handlers()?;
    let args = Args::parse();
    let local = tokio::task::LocalSet::new();
    local.run_until(run(args)).await
}

async fn run(args: Args) -> Result<(), BoxError> {
    let batch_size = normalize_batch_size(args.batch_size)?;
    match args.backend {
        Backend::Os => {
            let actor =
                common::open_os_actor(&args.device, args.bind, batch_size, args.payload_capacity)?;
            common::run_actor_tasks("udp-tokio-echo os", vec![actor], echo_actor::<OsUdpSocket>)
                .await
        }
        Backend::Xdp => {
            let bind = normalize_xdp_bind(&args.device, args.bind)?;
            let actors =
                common::open_xdp_wait_driven_actors(&args.device, bind, args.threads, batch_size)?;
            common::run_actor_tasks(
                "udp-tokio-echo xdp",
                actors,
                echo_actor::<WaitDrivenXdpUdpSocket>,
            )
            .await
        }
    }
}

async fn echo_actor<S>(
    handle: AsyncUdpHandle<S>,
    mut rx: AsyncUdpRx<S>,
    stop: Arc<AtomicBool>,
    total: Arc<AtomicU64>,
) -> Result<(), BoxError>
where
    S: FastUdpSocket<RecvMeta = UdpRecvMeta> + 'static,
    S::Driver: WaitDrivenDriverKind,
    S::RecvMeta: 'static,
    UdpRxBuffer<S>: PacketBufferMut<Frozen = UdpTxBuffer<S>>,
{
    let mut tx_packets: Vec<ActorTxPacket<S>> = Vec::with_capacity(DEFAULT_BATCH_SIZE);
    while !stop.load(Ordering::Relaxed) && !common::shutdown_requested() {
        let mut batch = match rx.recv_batch().await {
            Ok(batch) => batch,
            Err(_) => break,
        };

        tx_packets.clear();
        for packet in batch.drain() {
            let source = packet.meta.source;
            let source_port = packet.meta.destination_port;
            let mut tx = packet.into_transmit(source);
            tx.source_port = source_port;
            tx_packets.push(tx);
        }

        let sent = handle.send_tx_packets(&mut tx_packets).await?;
        total.fetch_add(sent as u64, Ordering::Relaxed);
    }
    Ok(())
}
