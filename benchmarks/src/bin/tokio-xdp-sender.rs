use std::net::{SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use fast_socket_async_rs::{
    ActorConfig, ActorTxMeta, AsyncUdpActor, AsyncUdpHandle, spawn_udp_actor_local,
};
use fast_socket_benchmarks::{
    BoxError, RunLimit, install_shutdown_signal_handlers, interface_selector, payload,
    shutdown_requested, write_sequence,
};
use fast_socket_rs::PacketBufferMut;
use fast_socket_xdp_rs::{
    PortFilter, RouteSnapshot, WaitDrivenXdpUdpSocket, XdpFactoryBuilder, XdpWorkerPlan,
};

const BLAST_BATCH_SIZE: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Mode {
    Blast,
}

#[derive(Debug, Parser)]
#[command(about = "Tokio AF_XDP UDP sender: blast packets through wait-driven XDP actors")]
struct Cli {
    /// Sender mode (only "blast" is supported).
    #[arg(value_enum)]
    mode: Mode,

    /// Interface index for the XDP queue(s).
    #[arg(long, conflicts_with = "iface")]
    ifindex: Option<u32>,

    /// Interface name for the XDP queue(s).
    #[arg(long, conflicts_with = "ifindex")]
    iface: Option<String>,

    /// Number of worker plans. All NIC queues are used and split into this
    /// many contiguous blocks. Must divide the queue count.
    #[arg(long, default_value_t = 1)]
    threads: usize,

    /// IPv4 local endpoint.
    #[arg(long)]
    local: SocketAddrV4,

    /// IPv4 destination endpoint.
    #[arg(long)]
    dest: SocketAddrV4,

    /// Per-packet payload bytes.
    #[arg(long, default_value_t = 64)]
    payload_len: usize,

    #[command(flatten)]
    limit: RunLimit,
}

type XdpActor = AsyncUdpActor<WaitDrivenXdpUdpSocket>;
type XdpActorHandle = AsyncUdpHandle<WaitDrivenXdpUdpSocket>;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    install_shutdown_signal_handlers()?;
    let cli = Cli::parse();
    let Mode::Blast = cli.mode;
    if cli.local.port() == 0 {
        return Err("--local port must be non-zero".into());
    }
    if cli.payload_len == 0 {
        return Err("--payload-len must be at least 1".into());
    }

    let local = tokio::task::LocalSet::new();
    local.run_until(run(cli)).await
}

async fn run(cli: Cli) -> Result<(), BoxError> {
    let selector = interface_selector(cli.ifindex, cli.iface)?;
    let routes = RouteSnapshot::from_netlink()?;
    let factory = XdpFactoryBuilder::new(selector)?
        .threads(cli.threads)
        .port_filter(PortFilter::UdpPorts(vec![cli.local.port()]))
        .route_snapshot(routes)
        .build()?;
    blast_all(
        factory.into_worker_plans(),
        cli.local,
        cli.dest,
        cli.payload_len,
        cli.limit,
    )
    .await
}

async fn blast_all(
    plans: Vec<XdpWorkerPlan>,
    local: SocketAddrV4,
    dest: SocketAddrV4,
    payload_len: usize,
    limit: RunLimit,
) -> Result<(), BoxError> {
    let actors = open_actors(plans, local)?;
    eprintln!(
        "tokio-xdp-sender blast: {} wait-driven actor socket(s)",
        actors.len()
    );

    let stop = Arc::new(AtomicBool::new(false));
    let queued = Arc::new(AtomicU64::new(0));
    let mut producer_tasks = Vec::with_capacity(actors.len());
    let mut rx_drain_tasks = Vec::with_capacity(actors.len());
    let mut actor_joins = Vec::with_capacity(actors.len());
    let mut shutdown_handles = Vec::with_capacity(actors.len());

    for (index, actor) in actors.into_iter().enumerate() {
        let handle = actor.handle();
        shutdown_handles.push(handle.clone());
        let (producer_handle, mut rx, join) = actor.into_parts();
        actor_joins.push(join);

        let producer_stop = Arc::clone(&stop);
        let producer_queued = Arc::clone(&queued);
        producer_tasks.push(tokio::task::spawn_local(async move {
            run_producer(
                index,
                producer_handle,
                dest.into(),
                payload_len,
                producer_stop,
                producer_queued,
            )
            .await
        }));

        rx_drain_tasks.push(tokio::task::spawn_local(async move {
            while rx.recv_batch().await.is_ok() {}
        }));
    }

    let started = Instant::now();
    let mut last_report = started;
    let mut last_count = 0u64;

    while !shutdown_requested() && !stop.load(Ordering::Relaxed) {
        let count = queued.load(Ordering::Relaxed);
        if !limit.keep_running(count, started) {
            break;
        }
        if actor_joins.iter().any(tokio::task::JoinHandle::is_finished) {
            stop.store(true, Ordering::Relaxed);
            break;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
        let now = Instant::now();
        if now.duration_since(last_report) >= Duration::from_secs(1) {
            let rate = (count - last_count) as f64 / now.duration_since(last_report).as_secs_f64();
            eprintln!(
                "tokio-xdp-sender blast: packets_queued/s={rate:.0} total_packets_queued={count}"
            );
            last_report = now;
            last_count = count;
        }
    }

    let elapsed = started.elapsed();
    stop.store(true, Ordering::Relaxed);

    for task in producer_tasks {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error),
            Err(error) => {
                return Err(format!("tokio-xdp-sender producer task failed: {error}").into());
            }
        }
    }

    for handle in &shutdown_handles {
        let _ = handle.shutdown().await;
    }
    drop(shutdown_handles);

    for join in actor_joins {
        match join.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(format!("tokio-xdp-sender actor failed: {error}").into()),
            Err(error) => return Err(format!("tokio-xdp-sender actor task failed: {error}").into()),
        }
    }

    for task in rx_drain_tasks {
        if let Err(error) = task.await {
            return Err(format!("tokio-xdp-sender RX drain task failed: {error}").into());
        }
    }

    let count = queued.load(Ordering::Relaxed);
    let rate = if elapsed.is_zero() {
        0.0
    } else {
        count as f64 / elapsed.as_secs_f64()
    };
    println!("tokio-xdp-sender blast: {count} packets queued in {elapsed:?} ({rate:.0} packets/s)");
    Ok(())
}

fn open_actors(plans: Vec<XdpWorkerPlan>, local: SocketAddrV4) -> Result<Vec<XdpActor>, BoxError> {
    let mut actors = Vec::new();
    for plan in plans {
        let aggregate = plan.open_udp_wait_driven_unpinned(local)?;
        for socket in aggregate.into_members() {
            actors.push(spawn_udp_actor_local(
                socket,
                ActorConfig {
                    recv_batch_size: BLAST_BATCH_SIZE,
                    ..ActorConfig::default()
                },
            )?);
        }
    }
    if actors.is_empty() {
        return Err("XDP factory did not produce any wait-driven UDP sockets".into());
    }
    Ok(actors)
}

async fn run_producer(
    index: usize,
    handle: XdpActorHandle,
    dest: SocketAddr,
    payload_len: usize,
    stop: Arc<AtomicBool>,
    queued: Arc<AtomicU64>,
) -> Result<(), BoxError> {
    let mut sequence = 0u64;
    let mut bytes = payload(payload_len);
    let mut tx_buffers = Vec::with_capacity(BLAST_BATCH_SIZE);

    while !stop.load(Ordering::Relaxed) && !shutdown_requested() {
        tx_buffers.clear();
        let allocated = handle
            .alloc_tx_batch(BLAST_BATCH_SIZE, &mut tx_buffers)
            .await?;
        if allocated == 0 {
            tokio::task::yield_now().await;
            continue;
        }

        for (offset, buffer) in tx_buffers.iter_mut().enumerate() {
            write_sequence(&mut bytes, sequence.wrapping_add(offset as u64));
            buffer.buffer_mut().extend_from_slice(&bytes)?;
        }

        let accepted = handle
            .send_tx_buffers(&mut tx_buffers, ActorTxMeta::new(dest))
            .await?;
        sequence = sequence.wrapping_add(accepted as u64);
        queued.fetch_add(accepted as u64, Ordering::Relaxed);
    }

    eprintln!("tokio-xdp-sender blast: producer {index} stopped");
    Ok(())
}
