#[path = "../common.rs"]
mod common;

use std::net::{
    IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6, UdpSocket as StdUdpSocket,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use common::{
    BoxError, Mode, bind_udp_socket_to_device, install_shutdown_signal_handlers,
    interface_ipv4_addr, payload, shutdown_requested, write_sequence,
};
use fast_socket_os_rs::{OsUdpSocket, OsUdpSocketConfig};
use fast_socket_rs::{
    BufferLayout, PacketBufferMut, QueueAffinity, QueueId, TxSlot, UdpSocket as FastUdpSocket,
    UdpTransmit, UdpTxBuffer, UdpTxBufferMut,
};
use fast_socket_xdp_rs::{
    BusyPollXdpUdpSocket, InterfaceSelector, PortFilter, RouteSnapshot, XdpFactoryBuilder,
    XdpPacketBuf, XdpPacketBufMut, XdpWorkerPlan, if_name_to_index,
};

const PAYLOAD_LEN: usize = 64;
const BATCH_SIZE: usize = 128;
const PROGRESS_INTERVAL: Duration = Duration::from_secs(1);
const COUNTER_FLUSH_PACKETS: u64 = BATCH_SIZE as u64;
const FINAL_DRAIN_TIMEOUT: Duration = Duration::from_millis(500);

#[repr(align(64))]
#[derive(Default)]
struct WorkerCounters {
    sent: AtomicU64,
}

#[derive(Debug, Parser)]
struct Args {
    /// Device name to attach or bind to.
    #[arg(long)]
    device: String,

    /// Target UDP endpoint as IP:PORT.
    #[arg(long)]
    target: SocketAddr,

    /// Source IP for the local socket address. Defaults to the IP on --device.
    #[arg(long)]
    source_ip: Option<IpAddr>,

    /// Socket backend to use.
    #[arg(long, value_enum, ignore_case = true)]
    mode: Mode,

    /// XDP mode only: number of worker threads. All NIC queues are used and
    /// split into this many contiguous blocks; each thread drives one aggregate
    /// socket over its queues/threads queues. Must divide the queue count.
    #[arg(long, default_value_t = 1)]
    threads: usize,

    /// Stop after this many milliseconds instead of waiting for Ctrl-C.
    #[arg(long)]
    duration_ms: Option<u64>,
}

fn main() -> Result<(), BoxError> {
    install_shutdown_signal_handlers()?;
    let args = Args::parse();

    match args.mode {
        Mode::Xdp => {
            let target = socket_addr_v4(args.target)?;
            let source_ip = source_ipv4_addr(&args.device, args.source_ip)?;
            run_xdp_blast(
                &args.device,
                source_ip,
                target,
                args.threads,
                args.duration_ms.map(Duration::from_millis),
            )
        }
        Mode::Os => {
            let mut socket = open_os_socket(&args.device, args.target, args.source_ip)?;
            blaster(
                &mut socket,
                args.target,
                args.duration_ms.map(Duration::from_millis),
            )
        }
    }
}

fn run_xdp_blast(
    device: &str,
    source_ip: Ipv4Addr,
    target: SocketAddrV4,
    threads: usize,
    duration: Option<Duration>,
) -> Result<(), BoxError> {
    let local = SocketAddrV4::new(source_ip, kernel_assigned_udp_port(source_ip)?);
    let routes = RouteSnapshot::from_netlink()?;
    // Phase 1: discover queues, attach the program, partition into `threads`
    // worker plans (one aggregate socket each over queues/threads queues).
    let factory = XdpFactoryBuilder::new(InterfaceSelector::Name(device.to_string()))?
        .threads(threads)
        .port_filter(PortFilter::UdpPorts(vec![local.port()]))
        .route_snapshot(routes)
        .build()?;
    let plans = factory.into_worker_plans();
    let worker_count = plans.len();
    eprintln!("blast xdp: {worker_count} aggregate socket(s) / thread(s)");

    let stop = Arc::new(AtomicBool::new(false));
    let start = Arc::new(AtomicBool::new(false));
    let (error_tx, error_rx) = mpsc::channel::<String>();
    let (ready_tx, ready_rx) = mpsc::channel::<u32>();
    let mut counters = Vec::with_capacity(worker_count);
    let mut handles = Vec::with_capacity(worker_count);

    for plan in plans {
        let worker_counters = Arc::new(WorkerCounters::default());
        counters.push(Arc::clone(&worker_counters));
        let worker_stop = Arc::clone(&stop);
        let worker_start = Arc::clone(&start);
        let worker_error_tx = error_tx.clone();
        let worker_ready_tx = ready_tx.clone();
        let cpu = plan.cpu();
        handles.push(thread::spawn(move || {
            if let Err(error) = run_xdp_blast_worker(
                plan,
                local,
                target,
                worker_stop.clone(),
                worker_start,
                worker_ready_tx,
                worker_counters,
            ) {
                let _ = worker_error_tx.send(format!("worker cpu {cpu}: {error}"));
                worker_stop.store(true, Ordering::Relaxed);
            }
        }));
    }
    drop(error_tx);
    drop(ready_tx);

    if let Err(error) = wait_for_workers_ready(worker_count, &ready_rx, &error_rx, &stop) {
        stop.store(true, Ordering::Relaxed);
        join_workers(handles)?;
        return Err(error);
    }

    let started = Instant::now();
    let mut last_report = started;
    let mut last_count = 0u64;
    start.store(true, Ordering::Release);

    loop {
        if let Ok(error) = error_rx.try_recv() {
            stop.store(true, Ordering::Relaxed);
            join_workers(handles)?;
            return Err(error.into());
        }

        if shutdown_requested() || duration.is_some_and(|duration| started.elapsed() >= duration) {
            break;
        }

        thread::sleep(Duration::from_millis(100));
        let now = Instant::now();
        if now.duration_since(last_report) >= PROGRESS_INTERVAL {
            let count = sum_sent(&counters);
            let interval = now.duration_since(last_report).as_secs_f64();
            let rate = (count - last_count) as f64 / interval;
            eprintln!("blast: {count} packets ({rate:.0} packets/s)");
            last_report = now;
            last_count = count;
        }
    }

    stop.store(true, Ordering::Relaxed);
    let elapsed = started.elapsed();
    join_workers(handles)?;
    if let Ok(error) = error_rx.try_recv() {
        return Err(error.into());
    }

    let count = sum_sent(&counters);
    let rate = if elapsed.is_zero() {
        0.0
    } else {
        count as f64 / elapsed.as_secs_f64()
    };
    println!("blast: {count} packets in {elapsed:?} ({rate:.0} packets/s)");
    Ok(())
}

/// Blasts round-robin across an aggregate's member queues until `stop`.
fn run_xdp_blast_worker(
    plan: XdpWorkerPlan,
    local: SocketAddrV4,
    target: SocketAddrV4,
    stop: Arc<AtomicBool>,
    start: Arc<AtomicBool>,
    ready_tx: mpsc::Sender<u32>,
    counters: Arc<WorkerCounters>,
) -> Result<(), BoxError> {
    let cpu = plan.cpu();
    // Pins to plan.cpu() and opens this worker's aggregate.
    let mut aggregate = plan.open_udp_busy_poll(local)?;
    let member_count = aggregate.len();

    signal_worker_ready(&ready_tx, cpu)?;
    if !wait_for_worker_start(&start, &stop) {
        return Ok(());
    }

    let mut next_socket = 0usize;
    let mut sequence = 0u64;
    let mut payload_bytes = payload(PAYLOAD_LEN);
    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let mut tx_buffers = Vec::with_capacity(BATCH_SIZE);
    let mut pending_completed = 0u64;
    let mut in_flight = 0u64;
    const SHUTDOWN_CHECK_MASK: u32 = 0xff;
    let mut shutdown_check_counter: u32 = 0;

    while !stop.load(Ordering::Relaxed) {
        let socket = &mut aggregate.members_mut()[next_socket];
        let (accepted, completed) = send_xdp_packet_batch(
            socket,
            target.into(),
            &mut payload_bytes,
            &mut sequence,
            &mut tx_buffers,
            &mut batch,
        )
        .map_err(|error| -> BoxError {
            format!("aggregate member {next_socket}: {error}").into()
        })?;
        in_flight += accepted as u64;
        if completed > 0 {
            let completed = completed as u64;
            in_flight = in_flight.saturating_sub(completed);
            pending_completed += completed;
            flush_worker_completions(&counters, &mut pending_completed, false);
        }
        next_socket = (next_socket + 1) % member_count;
        shutdown_check_counter = shutdown_check_counter.wrapping_add(1);
        if shutdown_check_counter & SHUTDOWN_CHECK_MASK == 0 && shutdown_requested() {
            break;
        }
    }

    let deadline = Instant::now() + FINAL_DRAIN_TIMEOUT;
    while in_flight > 0 && Instant::now() < deadline {
        let drained = aggregate.drain_tx_completions()? as u64;
        in_flight = in_flight.saturating_sub(drained);
        pending_completed += drained;
        if drained == 0 {
            thread::sleep(Duration::from_micros(50));
        }
    }

    flush_worker_completions(&counters, &mut pending_completed, true);
    Ok(())
}

fn send_xdp_packet_batch(
    socket: &mut BusyPollXdpUdpSocket,
    target: SocketAddr,
    payload_bytes: &mut [u8],
    sequence: &mut u64,
    tx_buffers: &mut Vec<XdpPacketBufMut>,
    batch: &mut Vec<TxSlot<UdpTransmit<XdpPacketBuf>>>,
) -> Result<(usize, usize), BoxError> {
    tx_buffers.clear();
    batch.clear();
    socket.allocate_tx_batch(tx_buffers, BATCH_SIZE)?;
    while let Some(mut packet) = tx_buffers.pop() {
        write_sequence(payload_bytes, *sequence);
        packet.extend_from_slice(payload_bytes)?;
        batch.push(TxSlot::Ready(UdpTransmit::new(packet.freeze(), target)));
        *sequence = sequence.wrapping_add(1);
    }

    if batch.is_empty() {
        let completed = socket.drain_tx_completions()?;
        return Ok((0, completed));
    }

    let accepted = socket.send(batch.as_mut_slice())?;
    if accepted < batch.len() {
        *sequence = sequence.wrapping_sub((batch.len() - accepted) as u64);
    }
    let completed = socket.drain_tx_completions()?;
    Ok((accepted, completed))
}

fn flush_worker_completions(counters: &WorkerCounters, pending: &mut u64, force: bool) {
    if *pending >= COUNTER_FLUSH_PACKETS || (force && *pending > 0) {
        counters.sent.fetch_add(*pending, Ordering::Relaxed);
        *pending = 0;
    }
}

fn wait_for_workers_ready(
    expected: usize,
    ready_rx: &mpsc::Receiver<u32>,
    error_rx: &mpsc::Receiver<String>,
    stop: &AtomicBool,
) -> Result<(), BoxError> {
    let mut ready = 0usize;
    while ready < expected {
        if let Ok(error) = error_rx.try_recv() {
            stop.store(true, Ordering::Relaxed);
            return Err(error.into());
        }

        match ready_rx.recv_timeout(Duration::from_millis(100)) {
            Ok(_) => ready += 1,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if shutdown_requested() {
                    stop.store(true, Ordering::Relaxed);
                    return Err("shutdown requested before XDP workers were ready".into());
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                stop.store(true, Ordering::Relaxed);
                if let Ok(error) = error_rx.try_recv() {
                    return Err(error.into());
                }
                return Err(format!(
                    "only {ready}/{expected} XDP workers became ready before the channel closed"
                )
                .into());
            }
        }
    }
    Ok(())
}

fn signal_worker_ready(ready_tx: &mpsc::Sender<u32>, cpu: u32) -> Result<(), BoxError> {
    ready_tx
        .send(cpu)
        .map_err(|error| format!("signal worker readiness: {error}").into())
}

fn wait_for_worker_start(start: &AtomicBool, stop: &AtomicBool) -> bool {
    while !start.load(Ordering::Acquire) && !stop.load(Ordering::Relaxed) && !shutdown_requested() {
        thread::yield_now();
    }
    start.load(Ordering::Acquire) && !stop.load(Ordering::Relaxed) && !shutdown_requested()
}

fn sum_sent(counters: &[Arc<WorkerCounters>]) -> u64 {
    counters
        .iter()
        .map(|counter| counter.sent.load(Ordering::Relaxed))
        .sum()
}

fn join_workers(handles: Vec<thread::JoinHandle<()>>) -> Result<(), BoxError> {
    for handle in handles {
        if handle.join().is_err() {
            return Err("blast worker thread panicked".into());
        }
    }
    Ok(())
}

fn blaster<S>(
    socket: &mut S,
    target: SocketAddr,
    duration: Option<Duration>,
) -> Result<(), BoxError>
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

    while !shutdown_requested() && duration.is_none_or(|duration| started.elapsed() < duration) {
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

fn open_os_socket(
    device: &str,
    target: SocketAddr,
    source_ip: Option<IpAddr>,
) -> Result<OsUdpSocket, BoxError> {
    let if_index = if_name_to_index(device)?;
    let socket = StdUdpSocket::bind(bind_addr(target, source_ip)?)?;
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

fn source_ipv4_addr(device: &str, source_ip: Option<IpAddr>) -> Result<Ipv4Addr, BoxError> {
    match source_ip {
        Some(IpAddr::V4(addr)) => Ok(addr),
        Some(IpAddr::V6(_)) => Err("XDP mode requires an IPv4 --source-ip".into()),
        None => interface_ipv4_addr(device),
    }
}

fn socket_addr_v4(addr: SocketAddr) -> Result<SocketAddrV4, BoxError> {
    match addr {
        SocketAddr::V4(addr) => Ok(addr),
        SocketAddr::V6(_) => Err("XDP mode requires an IPv4 target".into()),
    }
}

fn bind_addr(target: SocketAddr, source_ip: Option<IpAddr>) -> Result<SocketAddr, BoxError> {
    match (target, source_ip) {
        (SocketAddr::V4(_), Some(IpAddr::V4(addr))) => Ok(SocketAddrV4::new(addr, 0).into()),
        (SocketAddr::V6(_), Some(IpAddr::V6(addr))) => Ok(SocketAddrV6::new(addr, 0, 0, 0).into()),
        (SocketAddr::V4(_), Some(IpAddr::V6(_))) => {
            Err("IPv4 targets require an IPv4 --source-ip".into())
        }
        (SocketAddr::V6(_), Some(IpAddr::V4(_))) => {
            Err("IPv6 targets require an IPv6 --source-ip".into())
        }
        (SocketAddr::V4(_), None) => Ok(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0).into()),
        (SocketAddr::V6(_), None) => Ok(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0).into()),
    }
}

fn udp_payload_mtu(target: SocketAddr) -> usize {
    match target.ip() {
        IpAddr::V4(_) => 1472,
        IpAddr::V6(_) => 1452,
    }
}
