use std::net::{SocketAddr, SocketAddrV4};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use fast_socket_benchmarks::{
    BoxError, RunLimit, install_shutdown_signal_handlers, interface_selector, payload,
    shutdown_requested, write_sequence,
};
use fast_socket_rs::{PacketBufferMut, TxSlot, UdpSocket, UdpTransmit};
use fast_socket_xdp_rs::{
    BusyPollXdpUdpSocket, PortFilter, RouteSnapshot, XdpFactoryBuilder, XdpPacketBuf,
    XdpPacketBufMut, XdpWorkerPlan,
};

const BLAST_BATCH_SIZE: usize = 128;
const BLAST_COUNTER_FLUSH_PACKETS: u64 = BLAST_BATCH_SIZE as u64;
const FINAL_DRAIN_TIMEOUT: Duration = Duration::from_millis(500);

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Mode {
    Blast,
}

#[repr(align(64))]
#[derive(Default)]
struct WorkerCounters {
    sent: AtomicU64,
}

#[derive(Debug, Parser)]
#[command(about = "AF_XDP UDP sender: blast packets across one or many queues")]
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

    /// Number of worker threads. All NIC queues are used and split into this
    /// many contiguous blocks; each thread drives one aggregate socket over its
    /// queues/threads queues. Must divide the queue count.
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

fn main() -> Result<(), BoxError> {
    install_shutdown_signal_handlers()?;
    let cli = Cli::parse();
    let Mode::Blast = cli.mode;
    if cli.local.port() == 0 {
        return Err("--local port must be non-zero".into());
    }
    let selector = interface_selector(cli.ifindex, cli.iface)?;
    let routes = RouteSnapshot::from_netlink()?;
    // Phase 1: discover queues, attach the program, partition into T worker
    // plans (one aggregate socket each over Q/T queues sharing one UMEM).
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
}

fn blast_all(
    plans: Vec<XdpWorkerPlan>,
    local: SocketAddrV4,
    dest: SocketAddrV4,
    payload_len: usize,
    limit: RunLimit,
) -> Result<(), BoxError> {
    let worker_count = plans.len();
    eprintln!("xdp-sender blast: {worker_count} aggregate socket(s) / thread(s)");

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
            if let Err(error) = run_blast_worker(
                plan,
                local,
                dest,
                payload_len,
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
    start.store(true, Ordering::Release);
    let stats_stop = Arc::clone(&stop);
    let stats_counters = counters.clone();
    let stats = thread::spawn(move || blast_stats_loop(stats_counters, stats_stop));

    loop {
        if let Ok(error) = error_rx.try_recv() {
            stop.store(true, Ordering::Relaxed);
            join_workers(handles)?;
            let _ = stats.join();
            return Err(error.into());
        }

        let sent = sum_sent(&counters);
        if shutdown_requested() || !limit.keep_running(sent, started) {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }

    stop.store(true, Ordering::Relaxed);
    let elapsed = started.elapsed();
    join_workers(handles)?;
    let _ = stats.join();
    if let Ok(error) = error_rx.try_recv() {
        return Err(error.into());
    }

    let sent = sum_sent(&counters);
    let rate = if elapsed.is_zero() {
        0.0
    } else {
        sent as f64 / elapsed.as_secs_f64()
    };
    println!("xdp-sender blast: {sent} packets in {elapsed:?} ({rate:.0} packets/s)");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_blast_worker(
    plan: XdpWorkerPlan,
    local: SocketAddrV4,
    dest: SocketAddrV4,
    payload_len: usize,
    stop: Arc<AtomicBool>,
    start: Arc<AtomicBool>,
    ready_tx: mpsc::Sender<u32>,
    counters: Arc<WorkerCounters>,
) -> Result<(), BoxError> {
    let cpu = plan.cpu();
    // Pins to plan.cpu() and opens one aggregate socket over this worker's
    // queues, all sharing a single NUMA-local UMEM.
    let mut aggregate = plan.open_udp_busy_poll(local)?;
    let member_count = aggregate.len();

    signal_worker_ready(&ready_tx, cpu)?;
    if !wait_for_worker_start(&start, &stop) {
        return Ok(());
    }

    let mut next_socket = 0usize;
    let mut sequence = 0u64;
    let mut bytes = payload(payload_len);
    let mut batch = Vec::with_capacity(BLAST_BATCH_SIZE);
    let mut tx_buffers = Vec::with_capacity(BLAST_BATCH_SIZE);
    let mut pending_completed = 0u64;
    let mut in_flight = 0u64;
    const SHUTDOWN_CHECK_MASK: u32 = 0xff;
    let mut shutdown_check_counter: u32 = 0;

    while !stop.load(Ordering::Relaxed) {
        let socket = &mut aggregate.members_mut()[next_socket];
        let (accepted, completed) = send_packet_batch(
            socket,
            dest.into(),
            &mut bytes,
            &mut sequence,
            &mut tx_buffers,
            &mut batch,
        )
        .map_err(|error| -> BoxError {
            format!("aggregate member {next_socket}: {error}").into()
        })?;
        in_flight += accepted as u64;
        if completed > 0 {
            let completed_u64 = completed as u64;
            in_flight = in_flight.saturating_sub(completed_u64);
            pending_completed += completed_u64;
            flush_blast_completions(&counters, &mut pending_completed, false);
        }
        next_socket = (next_socket + 1) % member_count;
        shutdown_check_counter = shutdown_check_counter.wrapping_add(1);
        if shutdown_check_counter & SHUTDOWN_CHECK_MASK == 0 && shutdown_requested() {
            break;
        }
    }

    let deadline = Instant::now() + FINAL_DRAIN_TIMEOUT;
    while in_flight > 0 && Instant::now() < deadline {
        let drained_round = aggregate.drain_tx_completions()? as u64;
        in_flight = in_flight.saturating_sub(drained_round);
        pending_completed += drained_round;
        if drained_round == 0 {
            thread::sleep(Duration::from_micros(50));
        }
    }

    flush_blast_completions(&counters, &mut pending_completed, true);
    Ok(())
}

fn send_packet_batch(
    socket: &mut BusyPollXdpUdpSocket,
    dest: SocketAddr,
    bytes: &mut [u8],
    sequence: &mut u64,
    tx_buffers: &mut Vec<XdpPacketBufMut>,
    batch: &mut Vec<TxSlot<UdpTransmit<XdpPacketBuf>>>,
) -> Result<(usize, usize), BoxError> {
    tx_buffers.clear();
    batch.clear();
    socket.allocate_tx_batch(tx_buffers, BLAST_BATCH_SIZE)?;
    while let Some(mut packet) = tx_buffers.pop() {
        write_sequence(bytes, *sequence);
        packet.extend_from_slice(bytes)?;
        batch.push(TxSlot::Ready(UdpTransmit::new(packet.freeze(), dest)));
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

fn flush_blast_completions(counters: &WorkerCounters, pending: &mut u64, force: bool) {
    if *pending >= BLAST_COUNTER_FLUSH_PACKETS || (force && *pending > 0) {
        counters.sent.fetch_add(*pending, Ordering::Relaxed);
        *pending = 0;
    }
}

fn blast_stats_loop(counters: Vec<Arc<WorkerCounters>>, stop: Arc<AtomicBool>) {
    let mut prev_sent = 0u64;
    while !stop.load(Ordering::Relaxed) && !shutdown_requested() {
        thread::sleep(Duration::from_secs(1));
        if stop.load(Ordering::Relaxed) || shutdown_requested() {
            break;
        }
        let sent = sum_sent(&counters);
        eprintln!(
            "xdp-sender blast: packets_sent/s={} total_packets_sent={}",
            sent.saturating_sub(prev_sent),
            sent
        );
        prev_sent = sent;
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
            return Err("xdp-sender worker thread panicked".into());
        }
    }
    Ok(())
}
