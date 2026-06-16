#[path = "../common.rs"]
mod common;

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket as StdUdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;
use common::{
    BoxError, install_shutdown_signal_handlers, interface_ipv4_addr, payload, shutdown_requested,
    write_sequence,
};
use fast_socket_async_rs::{
    ActorConfig, ActorTxMeta, AsyncUdpActor, AsyncUdpHandle, spawn_udp_actor_local,
};
use fast_socket_rs::PacketBufferMut;
use fast_socket_xdp_rs::{
    InterfaceSelector, PortFilter, RouteSnapshot, WaitDrivenXdpUdpSocket, XdpFactoryBuilder,
};

const BATCH_SIZE: usize = 128;
const PAYLOAD_LEN: usize = 64;
const PROGRESS_INTERVAL: Duration = Duration::from_secs(1);
const COUNTER_FLUSH_PACKETS: u64 = BATCH_SIZE as u64;

#[derive(Debug, Parser)]
struct Args {
    /// Device name to attach to.
    #[arg(long)]
    device: String,

    /// Target UDP endpoint as IPv4:PORT.
    #[arg(long)]
    target: SocketAddrV4,

    /// Source IPv4 address for the local socket address. Defaults to the IP on --device.
    #[arg(long)]
    source_ip: Option<Ipv4Addr>,

    /// Number of XDP worker plans. All NIC queues are split into this many
    /// contiguous blocks. Must divide the queue count.
    #[arg(long, default_value_t = 1)]
    threads: usize,

    /// UDP payload length.
    #[arg(long, default_value_t = PAYLOAD_LEN)]
    payload_len: usize,

    /// Stop after this many milliseconds instead of waiting for Ctrl-C.
    #[arg(long)]
    duration_ms: Option<u64>,
}

type XdpActor = AsyncUdpActor<WaitDrivenXdpUdpSocket>;
type XdpActorHandle = AsyncUdpHandle<WaitDrivenXdpUdpSocket>;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    install_shutdown_signal_handlers()?;
    let args = Args::parse();
    if args.threads == 0 {
        return Err("--threads must be at least 1".into());
    }
    if args.payload_len == 0 {
        return Err("--payload-len must be at least 1".into());
    }

    let local = tokio::task::LocalSet::new();
    local
        .run_until(run_tokio_blast(
            &args.device,
            args.source_ip,
            args.target,
            args.threads,
            args.payload_len,
            args.duration_ms.map(Duration::from_millis),
        ))
        .await
}

async fn run_tokio_blast(
    device: &str,
    source_ip: Option<Ipv4Addr>,
    target: SocketAddrV4,
    threads: usize,
    payload_len: usize,
    duration: Option<Duration>,
) -> Result<(), BoxError> {
    let source_ip = match source_ip {
        Some(source_ip) => source_ip,
        None => interface_ipv4_addr(device)?,
    };
    let local = SocketAddrV4::new(source_ip, kernel_assigned_udp_port(source_ip)?);
    let routes = RouteSnapshot::from_netlink()?;
    let factory = XdpFactoryBuilder::new(InterfaceSelector::Name(device.to_string()))?
        .threads(threads)
        .port_filter(PortFilter::UdpPorts(vec![local.port()]))
        .route_snapshot(routes)
        .build()?;

    let mut actors = Vec::new();
    for plan in factory.into_worker_plans() {
        // Do not pin Tokio runtime workers from inside an async binary. The
        // actor itself is wait-driven, and backend-specific pinning belongs in
        // a dedicated runtime/worker setup.
        let aggregate = plan.open_udp_wait_driven_unpinned(local)?;
        for socket in aggregate.into_members() {
            actors.push(spawn_udp_actor_local(
                socket,
                ActorConfig {
                    recv_batch_size: BATCH_SIZE,
                    ..ActorConfig::default()
                },
            )?);
        }
    }

    if actors.is_empty() {
        return Err("XDP factory did not produce any wait-driven sockets".into());
    }

    eprintln!(
        "tokio-blast xdp: {} actor socket(s), sending {}-byte UDP payloads from {local} to {target}",
        actors.len(),
        payload_len
    );

    run_actor_producers(actors, target.into(), payload_len, duration).await
}

async fn run_actor_producers(
    actors: Vec<XdpActor>,
    target: SocketAddr,
    payload_len: usize,
    duration: Option<Duration>,
) -> Result<(), BoxError> {
    let stop = Arc::new(AtomicBool::new(false));
    let queued = Arc::new(AtomicU64::new(0));
    let mut producer_tasks = Vec::with_capacity(actors.len());
    let mut actor_joins = Vec::with_capacity(actors.len());
    let mut rx_drain_tasks = Vec::with_capacity(actors.len());
    let mut shutdown_handles = Vec::with_capacity(actors.len());

    for (index, actor) in actors.into_iter().enumerate() {
        let handle = actor.handle();
        shutdown_handles.push(handle.clone());
        let (producer_handle, mut rx, actor_join) = actor.into_parts();
        let stop_for_producer = Arc::clone(&stop);
        let queued_for_producer = Arc::clone(&queued);
        producer_tasks.push(tokio::task::spawn_local(async move {
            run_producer(
                index,
                producer_handle,
                target,
                payload_len,
                stop_for_producer,
                queued_for_producer,
            )
            .await
        }));
        rx_drain_tasks.push(tokio::task::spawn_local(async move {
            while rx.recv_batch().await.is_ok() {}
        }));
        actor_joins.push(actor_join);
    }

    let started = Instant::now();
    let mut last_report = started;
    let mut last_count = 0u64;

    while !shutdown_requested() && !stop.load(Ordering::Relaxed) {
        if duration.is_some_and(|duration| started.elapsed() >= duration) {
            break;
        }
        if actor_joins.iter().any(tokio::task::JoinHandle::is_finished) {
            stop.store(true, Ordering::Relaxed);
            break;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
        let now = Instant::now();
        if now.duration_since(last_report) >= PROGRESS_INTERVAL {
            let count = queued.load(Ordering::Relaxed);
            let interval = now.duration_since(last_report).as_secs_f64();
            let rate = (count - last_count) as f64 / interval;
            eprintln!("tokio-blast: {count} packets queued ({rate:.0} packets/s)");
            last_report = now;
            last_count = count;
        }
    }

    let stopped = Instant::now();
    stop.store(true, Ordering::Relaxed);
    for task in producer_tasks {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error),
            Err(error) => return Err(format!("tokio-blast producer task failed: {error}").into()),
        }
    }

    for handle in &shutdown_handles {
        let _ = handle.shutdown().await;
    }
    drop(shutdown_handles);

    for join in actor_joins {
        match join.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(format!("tokio-blast actor failed: {error}").into()),
            Err(error) => return Err(format!("tokio-blast actor task failed: {error}").into()),
        }
    }

    for task in rx_drain_tasks {
        if let Err(error) = task.await {
            return Err(format!("tokio-blast RX drain task failed: {error}").into());
        }
    }

    let elapsed = stopped.duration_since(started);
    let count = queued.load(Ordering::Relaxed);
    let rate = if elapsed.is_zero() {
        0.0
    } else {
        count as f64 / elapsed.as_secs_f64()
    };
    println!("tokio-blast: {count} packets queued in {elapsed:?} ({rate:.0} packets/s)");
    Ok(())
}

async fn run_producer(
    index: usize,
    handle: XdpActorHandle,
    target: SocketAddr,
    payload_len: usize,
    stop: Arc<AtomicBool>,
    queued: Arc<AtomicU64>,
) -> Result<(), BoxError> {
    let mut sequence = 0u64;
    let mut local_queued = 0u64;
    let mut payload_bytes = payload(payload_len);
    let mut tx_buffers = Vec::with_capacity(BATCH_SIZE);

    while !stop.load(Ordering::Relaxed) && !shutdown_requested() {
        tx_buffers.clear();
        let allocated = handle.alloc_tx_batch(BATCH_SIZE, &mut tx_buffers).await?;
        if allocated == 0 {
            tokio::task::yield_now().await;
            continue;
        }

        for (offset, buffer) in tx_buffers.iter_mut().enumerate() {
            write_sequence(&mut payload_bytes, sequence.wrapping_add(offset as u64));
            buffer.buffer_mut().extend_from_slice(&payload_bytes)?;
        }

        let accepted = handle
            .send_tx_buffers(&mut tx_buffers, ActorTxMeta::new(target))
            .await
            .map_err(|error| format!("producer {index}: {error}"))?;
        sequence = sequence.wrapping_add(allocated as u64);
        local_queued += accepted as u64;

        if local_queued >= COUNTER_FLUSH_PACKETS {
            queued.fetch_add(local_queued, Ordering::Relaxed);
            local_queued = 0;
        }
    }

    if local_queued != 0 {
        queued.fetch_add(local_queued, Ordering::Relaxed);
    }
    Ok(())
}

fn kernel_assigned_udp_port(local_ip: Ipv4Addr) -> Result<u16, BoxError> {
    let probe = StdUdpSocket::bind(SocketAddrV4::new(local_ip, 0))?;
    let port = probe.local_addr()?.port();
    drop(probe);
    Ok(port)
}
