use std::net::SocketAddrV4;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use fast_socket_benchmarks::{
    BoxError, Progress, RunLimit, install_shutdown_signal_handlers, interface_selector,
    parse_ipv4_udp, reflect_ipv4_udp, shutdown_requested,
};
use fast_socket_rs::{
    IpPacketSocket, IpPacketTransmit, PacketBuffer, PacketBufferMut, RawDevice, RecvBatch, TxSlot,
};
use fast_socket_xdp_rs::{
    BusyPollXdpIpPacketSocket, PortFilter, RouteSnapshot, XdpFactoryBuilder, XdpWorkerPlan,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Mode {
    Count,
    Pong,
}

#[derive(Debug, Parser)]
#[command(about = "AF_XDP IP packet listener: count or reflect received IPv4 UDP datagrams")]
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

    /// Number of worker threads. All NIC queues are used and split into this
    /// many contiguous blocks; each thread drives one aggregate socket over its
    /// queues/threads queues. Must divide the queue count.
    #[arg(long, default_value_t = 1)]
    threads: usize,

    #[command(flatten)]
    limit: RunLimit,
}

fn main() -> Result<(), BoxError> {
    install_shutdown_signal_handlers()?;
    let cli = Cli::parse();
    let mode = cli.mode;
    let bind = cli.bind;
    let limit = cli.limit;

    let selector = interface_selector(cli.ifindex, cli.iface)?;
    let routes = RouteSnapshot::from_netlink()?;
    // Phase 1: discover queues, attach the program with the bind port in the
    // filter, partition into T worker plans (one aggregate over Q/T queues).
    let factory = XdpFactoryBuilder::new(selector)?
        .threads(cli.threads)
        .port_filter(PortFilter::UdpPorts(vec![bind.port()]))
        .route_snapshot(routes.clone())
        .build()?;
    let plans = factory.into_worker_plans();
    eprintln!(
        "xdp-listener: {} aggregate socket(s) / thread(s)",
        plans.len()
    );

    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));
    let dropped = Arc::new(AtomicU64::new(0));
    let (error_tx, error_rx) = mpsc::channel::<String>();
    let mut handles = Vec::with_capacity(plans.len());

    for plan in plans {
        let worker_routes = routes.clone();
        let worker_stop = Arc::clone(&stop);
        let worker_total = Arc::clone(&total);
        let worker_dropped = Arc::clone(&dropped);
        let worker_error_tx = error_tx.clone();
        let cpu = plan.cpu();
        handles.push(thread::spawn(move || {
            if let Err(error) = run_worker(
                mode,
                plan,
                worker_routes,
                bind.port(),
                worker_stop.clone(),
                worker_total,
                worker_dropped,
            ) {
                let _ = worker_error_tx.send(format!("worker cpu {cpu}: {error}"));
                worker_stop.store(true, Relaxed);
            }
        }));
    }
    drop(error_tx);

    let started = Instant::now();
    let mut progress = Progress::new(match mode {
        Mode::Count => "xdp-listener count",
        Mode::Pong => "xdp-listener pong",
    });

    loop {
        let packets = total.load(Relaxed);
        if shutdown_requested() || !limit.keep_running(packets, started) {
            break;
        }
        if let Ok(error) = error_rx.try_recv() {
            stop.store(true, Relaxed);
            join_workers(handles)?;
            return Err(error.into());
        }
        progress.tick(packets);
        thread::sleep(Duration::from_millis(100));
    }

    stop.store(true, Relaxed);
    join_workers(handles)?;
    if let Ok(error) = error_rx.try_recv() {
        return Err(error.into());
    }
    let final_total = total.load(Relaxed);
    let final_dropped = dropped.load(Relaxed);
    progress.finish(final_total);
    if final_dropped > 0 {
        eprintln!(
            "xdp-listener: dropped {final_dropped} packets (port mismatch, parse failure, missing egress, or send failure)"
        );
    }
    Ok(())
}

fn run_worker(
    mode: Mode,
    plan: XdpWorkerPlan,
    routes: RouteSnapshot,
    bind_port: u16,
    stop: Arc<AtomicBool>,
    total: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
) -> Result<(), BoxError> {
    // Pins to plan.cpu() and opens one aggregate socket over this worker's
    // queues, all sharing a single NUMA-local UMEM.
    let mut aggregate = plan.open_ip_packet_busy_poll()?;

    let mut rx = RecvBatch::with_capacity(64);
    while !stop.load(Relaxed) && !shutdown_requested() {
        let mut delivered_this_pass = 0u64;
        // Per-member service: reflection must leave on the queue a frame arrived
        // on (each member owns its own UMEM), so pong walks members directly.
        for socket in aggregate.members_mut() {
            rx.clear();
            let received = socket.recv(&mut rx)?;
            if received == 0 {
                if mode == Mode::Pong {
                    socket.drain_tx_completions()?;
                }
                continue;
            }
            // Only frames that pass filtering count as forward progress. A
            // batch of packets for the wrong port, fragments, or destinations
            // with no egress would otherwise suppress the `spin_loop` hint
            // even when zero useful work was done.
            delivered_this_pass += match mode {
                Mode::Count => count_received(&mut rx, bind_port, &total, &dropped),
                Mode::Pong => pong_received(socket, &routes, bind_port, &total, &dropped, &mut rx)?,
            };
        }
        if delivered_this_pass == 0 {
            std::hint::spin_loop();
        }
    }
    Ok(())
}

fn count_received(
    rx: &mut RecvBatch<
        fast_socket_rs::IpPacketReceive<
            fast_socket_rs::IpPacketRxBuffer<BusyPollXdpIpPacketSocket>,
        >,
    >,
    bind_port: u16,
    total: &AtomicU64,
    dropped: &AtomicU64,
) -> u64 {
    let mut delivered = 0u64;
    for item in rx.drain() {
        if parse_ipv4_udp(item.packet.segments().next().unwrap_or_default())
            .is_some_and(|udp| udp.destination_port == bind_port)
        {
            total.fetch_add(1, Relaxed);
            delivered += 1;
        } else {
            dropped.fetch_add(1, Relaxed);
        }
    }
    delivered
}

fn pong_received(
    socket: &mut BusyPollXdpIpPacketSocket,
    routes: &RouteSnapshot,
    bind_port: u16,
    total: &AtomicU64,
    dropped: &AtomicU64,
    rx: &mut RecvBatch<
        fast_socket_rs::IpPacketReceive<
            fast_socket_rs::IpPacketRxBuffer<BusyPollXdpIpPacketSocket>,
        >,
    >,
) -> Result<u64, BoxError> {
    // Egress is queue-local; derive the member's interface + NIC queue from the
    // socket so reflected frames leave on the queue they arrived on.
    let ifindex = RawDevice::ifindex(socket);
    let queue = RawDevice::nic_queues(socket)[0];
    let mut delivered = 0u64;
    for mut item in rx.drain() {
        let Some(parsed) = parse_ipv4_udp(item.packet.segments().next().unwrap_or_default()) else {
            dropped.fetch_add(1, Relaxed);
            continue;
        };
        if parsed.destination_port != bind_port {
            dropped.fetch_add(1, Relaxed);
            continue;
        }
        if item.packet.len() > parsed.total_len {
            item.packet
                .trim_suffix(item.packet.len() - parsed.total_len)?;
        }
        let Some(destination) = reflect_ipv4_udp(item.packet.as_mut_slice()) else {
            dropped.fetch_add(1, Relaxed);
            continue;
        };
        let Some(egress) = routes.egress_v4_for_interface(destination, ifindex, queue) else {
            dropped.fetch_add(1, Relaxed);
            continue;
        };
        let mut tx = [TxSlot::Ready(IpPacketTransmit::new(
            item.packet.freeze(),
            egress,
        ))];
        if socket.send(&mut tx)? == 1 {
            total.fetch_add(1, Relaxed);
            delivered += 1;
        } else {
            dropped.fetch_add(1, Relaxed);
        }
    }
    socket.drain_tx_completions()?;
    Ok(delivered)
}

fn join_workers(handles: Vec<thread::JoinHandle<()>>) -> Result<(), BoxError> {
    for handle in handles {
        if handle.join().is_err() {
            return Err("xdp-listener worker thread panicked".into());
        }
    }
    Ok(())
}
