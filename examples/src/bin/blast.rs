#[path = "../common.rs"]
mod common;

use std::net::{
    IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6, UdpSocket as StdUdpSocket,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use common::{
    BoxError, Mode, bind_udp_socket_to_device, install_shutdown_signal_handlers,
    interface_ipv4_addr, payload, shutdown_requested, write_sequence,
};
use fast_socket_os_rs::{OsUdpSocket, OsUdpSocketConfig};
use fast_socket_rs::BusyPollDriver;
use fast_socket_rs::{
    BufferLayout, PacketBufferMut, QueueAffinity, QueueId, TxSlot, UdpSocket as FastUdpSocket,
    UdpTransmit, UdpTxBuffer, UdpTxBufferMut,
};
use fast_socket_xdp_rs::{
    InterfaceSelector, PortFilter, RouteSnapshot, XdpFactoryBuilder, XdpQueueLocalRouter,
    XdpRouteMonitor, XdpRouteMonitorHandle, if_name_to_index,
};

const PAYLOAD_LEN: usize = 64;
const BATCH_SIZE: usize = 64;
const PROGRESS_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Parser)]
struct Args {
    /// Device name to attach or bind to.
    #[arg(long)]
    device: String,

    /// Target UDP endpoint as IP:PORT.
    #[arg(long)]
    target: SocketAddr,

    /// Socket backend to use.
    #[arg(long, value_enum, ignore_case = true)]
    mode: Mode,

    /// XDP mode only: number of worker threads. All NIC queues are used and
    /// split into this many contiguous blocks; each thread drives one aggregate
    /// socket over its queues/threads queues. Must divide the queue count.
    #[arg(long, default_value_t = 1)]
    threads: usize,
}

fn main() -> Result<(), BoxError> {
    install_shutdown_signal_handlers()?;
    let args = Args::parse();

    match args.mode {
        Mode::Xdp => {
            let target = socket_addr_v4(args.target)?;
            run_xdp_blast(&args.device, target, args.threads)
        }
        Mode::Os => {
            let mut socket = open_os_socket(&args.device, args.target)?;
            blaster(&mut socket, args.target)
        }
    }
}

fn run_xdp_blast(device: &str, target: SocketAddrV4, threads: usize) -> Result<(), BoxError> {
    let local_ip = interface_ipv4_addr(device)?;
    let local = SocketAddrV4::new(local_ip, kernel_assigned_udp_port(local_ip)?);
    let routes = RouteSnapshot::from_netlink()?;
    let mut route_monitor = XdpRouteMonitor::new();
    // Phase 1: discover queues, attach the program, partition into `threads`
    // worker plans (one aggregate socket each over queues/threads queues).
    let factory = XdpFactoryBuilder::new(InterfaceSelector::Name(device.to_string()))?
        .threads(threads)
        .port_filter(PortFilter::UdpPorts(vec![local.port()]))
        .route_snapshot(routes)
        .build()?;
    let plans = factory.into_worker_plans();
    let monitor_queue = plans
        .first()
        .and_then(|plan| plan.queue_ids().first())
        .copied()
        .unwrap_or_else(|| QueueId::new(0));
    let mut workers = Vec::with_capacity(plans.len());
    for plan in plans {
        let route_updates = plan
            .queue_ids()
            .iter()
            .map(|_| route_monitor.register_queue())
            .collect::<Vec<_>>();
        workers.push((plan, route_updates));
    }
    let _route_monitor_thread = route_monitor.start_netlink(monitor_queue, Duration::from_secs(1));
    eprintln!(
        "blast xdp: {} aggregate socket(s) / thread(s)",
        workers.len()
    );

    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let mut handles = Vec::with_capacity(workers.len());
    for (plan, mut route_updates) in workers {
        let stop = Arc::clone(&stop);
        let total = Arc::clone(&total);
        let dest: SocketAddr = target.into();
        handles.push(thread::spawn(move || -> Result<(), String> {
            // Pins to plan.cpu() and opens this worker's aggregate.
            let mut aggregate = plan.open_udp_busy_poll(local).map_err(|e| e.to_string())?;
            blast_aggregate(&mut aggregate, &mut route_updates, dest, &stop, &total)
                .map_err(|e| e.to_string())
        }));
    }

    while !shutdown_requested() {
        thread::sleep(Duration::from_millis(200));
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for handle in handles {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => return Err("blast worker thread panicked".into()),
        }
    }
    let count = total.load(std::sync::atomic::Ordering::Relaxed);
    let elapsed = started.elapsed();
    let rate = if elapsed.is_zero() {
        0.0
    } else {
        count as f64 / elapsed.as_secs_f64()
    };
    println!("blast: {count} packets in {elapsed:?} ({rate:.0} packets/s)");
    Ok(())
}

/// Blasts round-robin across an aggregate's member queues until `stop`.
fn blast_aggregate(
    aggregate: &mut fast_socket_xdp_rs::XdpUdpAggregate<BusyPollDriver, XdpQueueLocalRouter>,
    route_updates: &mut [XdpRouteMonitorHandle],
    target: SocketAddr,
    stop: &AtomicBool,
    total: &AtomicU64,
) -> Result<(), BoxError> {
    use std::sync::atomic::Ordering::Relaxed;
    let member_count = aggregate.len();
    let mut next = 0usize;
    let mut sequence = 0u64;
    let mut payload_bytes = payload(PAYLOAD_LEN);
    let mut tx_buffers = Vec::with_capacity(BATCH_SIZE);
    let mut batch = Vec::with_capacity(BATCH_SIZE);
    debug_assert_eq!(route_updates.len(), member_count);

    while !stop.load(Relaxed) && !shutdown_requested() {
        let socket = &mut aggregate.members_mut()[next];
        route_updates[next].apply_updates(socket.routes_mut());
        tx_buffers.clear();
        batch.clear();
        socket.allocate_tx_batch(&mut tx_buffers, BATCH_SIZE)?;
        while let Some(mut packet) = tx_buffers.pop() {
            write_sequence(&mut payload_bytes, sequence);
            packet.extend_from_slice(&payload_bytes)?;
            batch.push(TxSlot::Ready(UdpTransmit::new(packet.freeze(), target)));
            sequence = sequence.wrapping_add(1);
        }
        if !batch.is_empty() {
            let accepted = socket.send(batch.as_mut_slice())? as u64;
            total.fetch_add(accepted, Relaxed);
        }
        socket.drain_tx_completions()?;
        next = (next + 1) % member_count;
    }
    Ok(())
}

fn blaster<S>(socket: &mut S, target: SocketAddr) -> Result<(), BoxError>
where
    S: FastUdpSocket,
{
    let started = Instant::now();
    let mut last_report = started;
    let mut last_count = 0u64;
    // Two counters:
    //   `next_sequence` is the next sequence number to stamp into payload
    //   bytes and advances by `batch.len()` whether or not the kernel
    //   accepted the slot — that way sequence numbers stay monotonic on the
    //   wire and a partial accept simply leaves gaps in the sequence space
    //   instead of replaying the same numbers.
    //   `count` is the count of slots the kernel actually accepted; that is
    //   what we report.
    let mut next_sequence = 0u64;
    let mut count = 0u64;
    let mut dropped = 0u64;
    let mut payload_bytes = payload(PAYLOAD_LEN);
    let mut tx_buffers: Vec<UdpTxBufferMut<S>> = Vec::with_capacity(BATCH_SIZE);
    let mut batch: Vec<TxSlot<UdpTransmit<UdpTxBuffer<S>>>> = Vec::with_capacity(BATCH_SIZE);

    while !shutdown_requested() {
        tx_buffers.clear();
        batch.clear();
        socket.allocate_tx_batch(&mut tx_buffers, BATCH_SIZE)?;

        while let Some(mut packet) = tx_buffers.pop() {
            write_sequence(&mut payload_bytes, next_sequence + batch.len() as u64);
            packet.extend_from_slice(&payload_bytes)?;
            batch.push(TxSlot::Ready(UdpTransmit::new(packet.freeze(), target)));
        }

        if batch.is_empty() {
            socket.drain_tx_completions()?;
            std::hint::spin_loop();
            continue;
        }

        let batch_len = batch.len() as u64;
        let accepted = socket.send(batch.as_mut_slice())?;
        if accepted < batch.len() {
            socket.drain_tx_completions()?;
        }
        next_sequence += batch_len;
        count += accepted as u64;
        dropped += batch_len - accepted as u64;

        let now = Instant::now();
        if now.duration_since(last_report) >= PROGRESS_INTERVAL {
            let interval = now.duration_since(last_report).as_secs_f64();
            let rate = (count - last_count) as f64 / interval;
            eprintln!("blast: {count} packets ({rate:.0} packets/s)");
            last_report = now;
            last_count = count;
        }
    }

    let elapsed = started.elapsed();
    let rate = if elapsed.is_zero() {
        0.0
    } else {
        count as f64 / elapsed.as_secs_f64()
    };
    println!("blast: {count} packets in {elapsed:?} ({rate:.0} packets/s)");
    if dropped > 0 {
        eprintln!("blast: dropped {dropped} packets to TX back-pressure (sequence-number gaps)");
    }
    Ok(())
}

fn kernel_assigned_udp_port(local_ip: Ipv4Addr) -> Result<u16, BoxError> {
    let probe = StdUdpSocket::bind(SocketAddrV4::new(local_ip, 0))?;
    let port = probe.local_addr()?.port();
    // `probe` drops here, releasing the port. The next bind on this port
    // may race with another process, but the kernel keeps the port in
    // TIME_WAIT briefly enough that example workloads almost always win
    // the race.
    drop(probe);
    Ok(port)
}

fn open_os_socket(device: &str, target: SocketAddr) -> Result<OsUdpSocket, BoxError> {
    let if_index = if_name_to_index(device)?;
    let socket = StdUdpSocket::bind(unspecified_addr(target))?;
    bind_udp_socket_to_device(&socket, device)?;

    let layout = BufferLayout::with_headroom_and_tailroom(PAYLOAD_LEN.max(2048), 0, 0);
    Ok(OsUdpSocket::from_std(
        socket,
        OsUdpSocketConfig {
            if_index: Some(if_index),
            queue_id: QueueId::new(0),
            queue_affinity: QueueAffinity::Any,
            rx_buffer_layout: layout,
            tx_buffer_layout: layout,
            mtu: udp_payload_mtu(target),
            ..Default::default()
        },
    )?)
}

fn socket_addr_v4(addr: SocketAddr) -> Result<SocketAddrV4, BoxError> {
    match addr {
        SocketAddr::V4(addr) => Ok(addr),
        SocketAddr::V6(_) => Err("XDP mode requires an IPv4 target".into()),
    }
}

fn unspecified_addr(target: SocketAddr) -> SocketAddr {
    match target {
        SocketAddr::V4(_) => SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0).into(),
        SocketAddr::V6(_) => SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0).into(),
    }
}

fn udp_payload_mtu(target: SocketAddr) -> usize {
    match target.ip() {
        IpAddr::V4(_) => 1472,
        IpAddr::V6(_) => 1452,
    }
}
