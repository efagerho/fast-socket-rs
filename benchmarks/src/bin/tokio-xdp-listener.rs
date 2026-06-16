use std::net::SocketAddrV4;
use std::os::fd::OwnedFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use fast_socket_benchmarks::{
    BoxError, Progress, RunLimit, install_shutdown_signal_handlers, interface_selector,
    parse_ipv4_udp, reflect_ipv4_udp, shutdown_requested,
};
use fast_socket_rs::{
    IpPacketReceive, IpPacketRxBuffer, IpPacketSocket, IpPacketTransmit, PacketBuffer,
    PacketBufferMut, PollDriver, RawDevice, RecvBatch, TxSlot,
};
use fast_socket_xdp_rs::{
    PortFilter, RouteSnapshot, WaitDrivenXdpIpPacketSocket, XdpFactoryBuilder, XdpWorkerPlan,
};
use tokio::io::unix::AsyncFd;

const RX_BATCH_SIZE: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Mode {
    Count,
    Pong,
}

#[derive(Debug, Parser)]
#[command(about = "Tokio AF_XDP IP packet listener: count or reflect IPv4 UDP datagrams")]
struct Cli {
    /// Listen mode.
    #[arg(value_enum)]
    mode: Mode,

    /// Interface index whose XDP queues should own the sockets.
    #[arg(long, conflicts_with = "iface")]
    ifindex: Option<u32>,

    /// Interface name whose XDP queues should own the sockets.
    #[arg(long, conflicts_with = "ifindex")]
    iface: Option<String>,

    /// IPv4 bind endpoint.
    #[arg(long)]
    bind: SocketAddrV4,

    /// Number of worker plans. All NIC queues are used and split into this
    /// many contiguous blocks. Must divide the queue count.
    #[arg(long, default_value_t = 1)]
    threads: usize,

    #[command(flatten)]
    limit: RunLimit,
}

type RxBatch = RecvBatch<
    IpPacketReceive<
        IpPacketRxBuffer<WaitDrivenXdpIpPacketSocket>,
        <WaitDrivenXdpIpPacketSocket as IpPacketSocket>::RecvMeta,
    >,
>;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BoxError> {
    install_shutdown_signal_handlers()?;
    let cli = Cli::parse();
    let local = tokio::task::LocalSet::new();
    local.run_until(run(cli)).await
}

async fn run(cli: Cli) -> Result<(), BoxError> {
    let mode = cli.mode;
    let bind = cli.bind;
    let limit = cli.limit;
    let selector = interface_selector(cli.ifindex, cli.iface)?;
    let routes = RouteSnapshot::from_netlink()?;
    let factory = XdpFactoryBuilder::new(selector)?
        .threads(cli.threads)
        .port_filter(PortFilter::UdpPorts(vec![bind.port()]))
        .route_snapshot(routes.clone())
        .build()?;
    let sockets = open_sockets(factory.into_worker_plans())?;
    eprintln!(
        "tokio-xdp-listener: {} wait-driven IP packet socket(s)",
        sockets.len()
    );

    run_sockets(mode, sockets, routes, bind.port(), limit).await
}

fn open_sockets(plans: Vec<XdpWorkerPlan>) -> Result<Vec<WaitDrivenXdpIpPacketSocket>, BoxError> {
    let mut sockets = Vec::new();
    for plan in plans {
        let aggregate = plan.open_ip_packet_wait_driven_unpinned()?;
        sockets.extend(aggregate.into_members());
    }
    if sockets.is_empty() {
        return Err("XDP factory did not produce any wait-driven IP packet sockets".into());
    }
    Ok(sockets)
}

async fn run_sockets(
    mode: Mode,
    sockets: Vec<WaitDrivenXdpIpPacketSocket>,
    routes: RouteSnapshot,
    bind_port: u16,
    limit: RunLimit,
) -> Result<(), BoxError> {
    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));
    let dropped = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::with_capacity(sockets.len());

    for socket in sockets {
        let worker_routes = routes.clone();
        let worker_stop = Arc::clone(&stop);
        let worker_total = Arc::clone(&total);
        let worker_dropped = Arc::clone(&dropped);
        tasks.push(tokio::task::spawn_local(async move {
            run_socket(
                mode,
                socket,
                worker_routes,
                bind_port,
                worker_stop,
                worker_total,
                worker_dropped,
            )
            .await
        }));
    }

    let started = Instant::now();
    let mut progress = Progress::new(match mode {
        Mode::Count => "tokio-xdp-listener count",
        Mode::Pong => "tokio-xdp-listener pong",
    });

    while !shutdown_requested() && !stop.load(Ordering::Relaxed) {
        let packets = total.load(Ordering::Relaxed);
        if !limit.keep_running(packets, started) {
            break;
        }
        if tasks.iter().any(tokio::task::JoinHandle::is_finished) {
            break;
        }
        progress.tick(packets);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    stop.store(true, Ordering::Relaxed);
    for task in tasks {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error),
            Err(error) => return Err(format!("tokio-xdp-listener task failed: {error}").into()),
        }
    }

    let final_total = total.load(Ordering::Relaxed);
    let final_dropped = dropped.load(Ordering::Relaxed);
    progress.finish(final_total);
    if final_dropped > 0 {
        eprintln!(
            "tokio-xdp-listener: dropped {final_dropped} packets (port mismatch, parse failure, missing egress, or send failure)"
        );
    }
    Ok(())
}

async fn run_socket(
    mode: Mode,
    mut socket: WaitDrivenXdpIpPacketSocket,
    routes: RouteSnapshot,
    bind_port: u16,
    stop: Arc<AtomicBool>,
    total: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
) -> Result<(), BoxError> {
    let wait_fd = socket_wait_fd(&socket)?;
    let mut rx: RxBatch = RecvBatch::with_capacity(RX_BATCH_SIZE);

    while !stop.load(Ordering::Relaxed) && !shutdown_requested() {
        rx.clear();
        let received = socket.recv(&mut rx)?;
        if received != 0 {
            match mode {
                Mode::Count => {
                    count_received(&mut rx, bind_port, &total, &dropped);
                }
                Mode::Pong => {
                    pong_received(&mut socket, &routes, bind_port, &total, &dropped, &mut rx)?;
                }
            }
            continue;
        }

        if mode == Mode::Pong {
            socket.drain_tx_completions()?;
        }

        tokio::select! {
            ready = wait_fd.readable() => {
                let mut guard = ready?;
                guard.clear_ready();
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }

    Ok(())
}

fn socket_wait_fd(socket: &WaitDrivenXdpIpPacketSocket) -> Result<AsyncFd<OwnedFd>, BoxError> {
    let wake = socket
        .driver()
        .wake_handle()
        .ok_or("wait-driven XDP IP socket did not expose a wake fd")?;
    let fd = wake.borrowed_fd().try_clone_to_owned()?;
    Ok(AsyncFd::new(fd)?)
}

fn count_received(rx: &mut RxBatch, bind_port: u16, total: &AtomicU64, dropped: &AtomicU64) -> u64 {
    let mut delivered = 0u64;
    for item in rx.drain() {
        if parse_ipv4_udp(item.packet.segments().next().unwrap_or_default())
            .is_some_and(|udp| udp.destination_port == bind_port)
        {
            total.fetch_add(1, Ordering::Relaxed);
            delivered += 1;
        } else {
            dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
    delivered
}

fn pong_received(
    socket: &mut WaitDrivenXdpIpPacketSocket,
    routes: &RouteSnapshot,
    bind_port: u16,
    total: &AtomicU64,
    dropped: &AtomicU64,
    rx: &mut RxBatch,
) -> Result<u64, BoxError> {
    let ifindex = RawDevice::ifindex(socket);
    let queue = RawDevice::nic_queues(socket)[0];
    let mut delivered = 0u64;
    for mut item in rx.drain() {
        let Some(parsed) = parse_ipv4_udp(item.packet.segments().next().unwrap_or_default()) else {
            dropped.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        if parsed.destination_port != bind_port {
            dropped.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        if item.packet.len() > parsed.total_len {
            item.packet
                .trim_suffix(item.packet.len() - parsed.total_len)?;
        }
        let Some(destination) = reflect_ipv4_udp(item.packet.as_mut_slice()) else {
            dropped.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        let Some(egress) = routes.egress_v4_for_interface(destination, ifindex, queue) else {
            dropped.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        let mut tx = [TxSlot::Ready(IpPacketTransmit::new(
            item.packet.freeze(),
            egress,
        ))];
        if socket.send(&mut tx)? == 1 {
            socket.notify_tx()?;
            total.fetch_add(1, Ordering::Relaxed);
            delivered += 1;
        } else {
            dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
    socket.drain_tx_completions()?;
    Ok(delivered)
}
