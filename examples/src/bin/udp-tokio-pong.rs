#[path = "../common.rs"]
mod common;

use std::net::{SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use clap::Parser;
use common::{
    Backend, BoxError, DEFAULT_BATCH_SIZE, DEFAULT_THREADS, install_shutdown_signal_handlers,
    normalize_batch_size, normalize_payload_len, normalize_xdp_bind, payload,
};
use fast_socket_async_rs::{ActorTxBuffer, ActorTxPacket, AsyncUdpHandle, AsyncUdpRx};
use fast_socket_rs::{
    PacketBufferMut, UdpRecvMeta, UdpSocket as FastUdpSocket, WaitDrivenDriverKind,
};

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

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    install_shutdown_signal_handlers()?;
    let args = Args::parse();
    let local = tokio::task::LocalSet::new();
    local.run_until(run(args)).await
}

async fn run(args: Args) -> Result<(), BoxError> {
    let batch_size = normalize_batch_size(args.batch_size)?;
    let response_len = normalize_payload_len(args.response_len)?;
    let response = Arc::new(payload(response_len));
    match args.backend {
        Backend::Os => {
            let actor = common::open_os_actor(&args.device, args.bind, batch_size, response_len)?;
            common::run_actor_tasks("udp-tokio-pong os", vec![actor], {
                let response = Arc::clone(&response);
                move |handle, rx, stop, total| {
                    pong_actor(handle, rx, stop, total, Arc::clone(&response), batch_size)
                }
            })
            .await
        }
        Backend::Xdp => {
            let bind = normalize_xdp_bind(&args.device, args.bind)?;
            let actors =
                common::open_xdp_wait_driven_actors(&args.device, bind, args.threads, batch_size)?;
            common::run_actor_tasks("udp-tokio-pong xdp", actors, {
                let response = Arc::clone(&response);
                move |handle, rx, stop, total| {
                    pong_actor(handle, rx, stop, total, Arc::clone(&response), batch_size)
                }
            })
            .await
        }
    }
}

async fn pong_actor<S>(
    handle: AsyncUdpHandle<S>,
    mut rx: AsyncUdpRx<S>,
    stop: Arc<AtomicBool>,
    total: Arc<AtomicU64>,
    response: Arc<Vec<u8>>,
    batch_size: usize,
) -> Result<(), BoxError>
where
    S: FastUdpSocket<RecvMeta = UdpRecvMeta> + 'static,
    S::Driver: WaitDrivenDriverKind,
    S::RecvMeta: 'static,
{
    let mut destinations: Vec<(SocketAddr, Option<u16>)> = Vec::with_capacity(batch_size);
    let mut tx_buffers: Vec<ActorTxBuffer<S>> = Vec::with_capacity(batch_size);
    let mut tx_packets: Vec<ActorTxPacket<S>> = Vec::with_capacity(batch_size);

    while !stop.load(Ordering::Relaxed) && !common::shutdown_requested() {
        let mut batch = match rx.recv_batch().await {
            Ok(batch) => batch,
            Err(_) => break,
        };

        destinations.clear();
        for packet in batch.drain() {
            destinations.push((packet.meta.source, packet.meta.destination_port));
        }

        tx_packets.clear();
        while tx_packets.len() < destinations.len()
            && !stop.load(Ordering::Relaxed)
            && !common::shutdown_requested()
        {
            tx_buffers.clear();
            let allocated = handle
                .alloc_tx_batch(destinations.len() - tx_packets.len(), &mut tx_buffers)
                .await?;
            if allocated == 0 {
                tokio::task::yield_now().await;
                continue;
            }

            for mut buffer in tx_buffers.drain(..) {
                let Some((destination, source_port)) = destinations.get(tx_packets.len()).copied()
                else {
                    break;
                };
                buffer.buffer_mut().extend_from_slice(&response)?;
                let mut packet = buffer.freeze(destination);
                packet.source_port = source_port;
                tx_packets.push(packet);
            }
        }

        let sent = handle.send_tx_packets(&mut tx_packets).await?;
        total.fetch_add(sent as u64, Ordering::Relaxed);
    }
    Ok(())
}
