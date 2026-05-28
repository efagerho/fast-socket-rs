use std::collections::BTreeMap;
use std::net::{SocketAddr, SocketAddrV4};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use fast_socket_benchmarks::{
    BoxError, Progress, RunLimit, XdpProgramMap, attach_xdp_programs_for_slots,
    install_shutdown_signal_handlers, payload, pin_current_thread_to_cpu, shutdown_requested,
    write_sequence, xdp_program_for_slot,
};
use fast_socket_rs::{
    BufferPool, IfIndex, PacketBufferMut, QueueId, TxSlot, UdpSocket, UdpTransmit,
};
use fast_socket_xdp_rs::{
    BusyPollXdpUdpSocket, RouteSnapshot, XdpPacketBuf, XdpPacketBufMut, XdpProgramHandle,
    XdpQueueSlot, XdpUdpSocket, cpu_for_xdp_queue, if_index_to_name, resolve_xdp_queue_slot,
    xdp_queue_slots_for_interface,
};

const BLAST_BATCH_SIZE: usize = 128;
const BLAST_COUNTER_FLUSH_PACKETS: u64 = BLAST_BATCH_SIZE as u64;
const FINAL_DRAIN_TIMEOUT: Duration = Duration::from_millis(500);

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Mode {
    Blast,
}

struct QueueGroup {
    cpu: u32,
    slots: Vec<XdpQueueSlot>,
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

    /// Specific XDP queue to attach to (default 0). Mutually exclusive with --all-queues.
    #[arg(long, conflicts_with = "all_queues")]
    queue: Option<u32>,

    /// Spread the workload across every queue on the interface.
    #[arg(long, conflicts_with = "queue")]
    all_queues: bool,

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

    if cli.all_queues {
        let slots = queue_slots_from_cli(cli.ifindex, cli.iface)?;
        blast_all(slots, cli.local, cli.dest, cli.payload_len, cli.limit)
    } else {
        let queue = QueueId::new(cli.queue.unwrap_or(0));
        let slot = queue_slot_from_cli(cli.ifindex, cli.iface, queue)?;
        let routes = RouteSnapshot::from_netlink()?;
        let mut socket = open_udp_socket(&slot, &routes, cli.local, cli.dest, None)?;
        blast(&mut socket, cli.dest.into(), cli.payload_len, cli.limit)
    }
}

fn queue_slot_from_cli(
    ifindex: Option<u32>,
    iface: Option<String>,
    queue: QueueId,
) -> Result<XdpQueueSlot, BoxError> {
    let iface = match (ifindex.map(IfIndex::new), iface) {
        (Some(ifindex), None) => if_index_to_name(ifindex)?,
        (None, Some(iface)) => iface,
        (Some(_), Some(_)) => return Err("use only one of --ifindex or --iface".into()),
        (None, None) => return Err("missing --ifindex N or --iface NAME".into()),
    };
    Ok(resolve_xdp_queue_slot(&iface, queue)?)
}

fn queue_slots_from_cli(
    ifindex: Option<u32>,
    iface: Option<String>,
) -> Result<Vec<XdpQueueSlot>, BoxError> {
    let iface = match (ifindex.map(IfIndex::new), iface) {
        (Some(ifindex), None) => if_index_to_name(ifindex)?,
        (None, Some(iface)) => iface,
        (Some(_), Some(_)) => return Err("use only one of --ifindex or --iface".into()),
        (None, None) => return Err("missing --ifindex N or --iface NAME".into()),
    };
    Ok(xdp_queue_slots_for_interface(&iface)?)
}

fn open_udp_socket(
    slot: &XdpQueueSlot,
    routes: &RouteSnapshot,
    local: SocketAddrV4,
    dest: SocketAddrV4,
    program: Option<&XdpProgramHandle>,
) -> Result<BusyPollXdpUdpSocket, BoxError> {
    let egress = routes
        .egress_v4_for_interface(*dest.ip(), slot.ifindex, slot.queue)
        .ok_or_else(|| format!("no queue-local netlink route/ARP entry for {}", dest.ip()))?;
    let mut builder = XdpUdpSocket::builder(slot.ifindex, slot.queue, local)
        .mtu(egress.mtu as usize)
        .route_snapshot(routes.clone())
        .bind_udp_port(local.port());
    if let Some(program) = program {
        builder = builder.attached_program(program.clone());
    }
    Ok(builder.open_busy_poll()?)
}

fn blast<S>(
    socket: &mut S,
    dest: SocketAddr,
    payload_len: usize,
    limit: RunLimit,
) -> Result<(), BoxError>
where
    S: UdpSocket<TxPool = fast_socket_xdp_rs::XdpTxPool>,
{
    let started = Instant::now();
    let mut progress = Progress::new("xdp-sender blast");
    let mut completed: u64 = 0;
    let mut in_flight: u64 = 0;
    let mut bytes = payload(payload_len);
    while limit.keep_running(completed, started) && !shutdown_requested() {
        write_sequence(&mut bytes, completed.wrapping_add(in_flight));
        let Some(mut packet) = socket.tx_pool_mut().allocate() else {
            let drained = socket.drain_tx_completions()? as u64;
            in_flight = in_flight.saturating_sub(drained);
            completed += drained;
            std::hint::spin_loop();
            continue;
        };
        packet.extend_from_slice(&bytes)?;
        let mut batch = [TxSlot::Ready(UdpTransmit::new(packet.freeze(), dest))];
        match socket.send(&mut batch) {
            Ok(1) => in_flight += 1,
            Ok(_) => std::hint::spin_loop(),
            Err(error) => return Err(error.into()),
        }
        let drained = socket.drain_tx_completions()? as u64;
        in_flight = in_flight.saturating_sub(drained);
        completed += drained;
        progress.tick(completed);
    }
    drain_remaining(socket, &mut in_flight, &mut completed)?;
    progress.finish(completed);
    Ok(())
}

fn drain_remaining<S: UdpSocket>(
    socket: &mut S,
    in_flight: &mut u64,
    completed: &mut u64,
) -> Result<(), BoxError> {
    let deadline = Instant::now() + FINAL_DRAIN_TIMEOUT;
    while *in_flight > 0 && Instant::now() < deadline {
        let drained = socket.drain_tx_completions()? as u64;
        *in_flight = in_flight.saturating_sub(drained);
        *completed += drained;
        if drained == 0 {
            thread::sleep(Duration::from_micros(50));
        }
    }
    Ok(())
}

fn blast_all(
    slots: Vec<XdpQueueSlot>,
    local: SocketAddrV4,
    dest: SocketAddrV4,
    payload_len: usize,
    limit: RunLimit,
) -> Result<(), BoxError> {
    validate_blast_flow_ports(local, &slots)?;

    let programs = Arc::new(attach_xdp_programs_for_slots(&slots)?);
    let groups = queue_groups_by_cpu(slots)?;
    let total_slots = groups.iter().map(|group| group.slots.len()).sum::<usize>();
    eprintln!(
        "xdp-sender blast: {total_slots} queue sockets coalesced onto {} CPU threads",
        groups.len()
    );

    let routes = RouteSnapshot::from_netlink()?;
    let stop = Arc::new(AtomicBool::new(false));
    let start = Arc::new(AtomicBool::new(false));
    let (error_tx, error_rx) = mpsc::channel::<String>();
    let (ready_tx, ready_rx) = mpsc::channel::<u32>();
    let mut counters = Vec::with_capacity(groups.len());
    let mut handles = Vec::with_capacity(groups.len());
    let worker_count = groups.len();

    for group in groups {
        let worker_counters = Arc::new(WorkerCounters::default());
        counters.push(Arc::clone(&worker_counters));
        let worker_routes = routes.clone();
        let worker_stop = Arc::clone(&stop);
        let worker_start = Arc::clone(&start);
        let worker_error_tx = error_tx.clone();
        let worker_ready_tx = ready_tx.clone();
        let worker_programs = Arc::clone(&programs);
        handles.push(thread::spawn(move || {
            if let Err(error) = run_blast_worker(
                group.cpu,
                group.slots,
                worker_routes,
                worker_programs,
                local,
                dest,
                payload_len,
                worker_stop.clone(),
                worker_start,
                worker_ready_tx,
                worker_counters,
            ) {
                let _ = worker_error_tx.send(format!("worker cpu {}: {error}", group.cpu));
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
    cpu: u32,
    slots: Vec<XdpQueueSlot>,
    routes: RouteSnapshot,
    programs: Arc<XdpProgramMap>,
    local: SocketAddrV4,
    dest: SocketAddrV4,
    payload_len: usize,
    stop: Arc<AtomicBool>,
    start: Arc<AtomicBool>,
    ready_tx: mpsc::Sender<u32>,
    counters: Arc<WorkerCounters>,
) -> Result<(), BoxError> {
    pin_current_thread_to_cpu(cpu)?;

    let mut sockets = Vec::with_capacity(slots.len());
    for slot in slots {
        let local = local_for_blast_slot(local, slot.flat_index)?;
        let program = xdp_program_for_slot(&programs, &slot)?;
        let socket = open_udp_socket(&slot, &routes, local, dest, Some(program))?;
        sockets.push((slot, local, socket));
    }

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
        let (_, _, socket) = &mut sockets[next_socket];
        let (accepted, completed) = send_packet_batch(
            socket,
            dest.into(),
            &mut bytes,
            &mut sequence,
            &mut tx_buffers,
            &mut batch,
        )
        .map_err(|error| describe_socket_error(&sockets, next_socket, &error))?;
        in_flight += accepted as u64;
        if completed > 0 {
            let completed_u64 = completed as u64;
            in_flight = in_flight.saturating_sub(completed_u64);
            pending_completed += completed_u64;
            flush_blast_completions(&counters, &mut pending_completed, false);
        }
        next_socket = (next_socket + 1) % sockets.len();
        shutdown_check_counter = shutdown_check_counter.wrapping_add(1);
        if shutdown_check_counter & SHUTDOWN_CHECK_MASK == 0 && shutdown_requested() {
            break;
        }
    }

    let deadline = Instant::now() + FINAL_DRAIN_TIMEOUT;
    while in_flight > 0 && Instant::now() < deadline {
        let mut drained_round = 0u64;
        for (_, _, socket) in &mut sockets {
            let drained = socket.drain_tx_completions()? as u64;
            drained_round += drained;
        }
        in_flight = in_flight.saturating_sub(drained_round);
        pending_completed += drained_round;
        if drained_round == 0 {
            thread::sleep(Duration::from_micros(50));
        }
    }

    flush_blast_completions(&counters, &mut pending_completed, true);
    Ok(())
}

fn describe_socket_error(
    sockets: &[(XdpQueueSlot, SocketAddrV4, BusyPollXdpUdpSocket)],
    index: usize,
    error: &BoxError,
) -> BoxError {
    let (slot, local, _) = &sockets[index];
    format!(
        "{} queue {} flat {} local {}: {error}",
        slot.iface,
        slot.queue.get(),
        slot.flat_index.get(),
        local
    )
    .into()
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

fn queue_groups_by_cpu(slots: Vec<XdpQueueSlot>) -> Result<Vec<QueueGroup>, BoxError> {
    let mut by_cpu: BTreeMap<u32, Vec<XdpQueueSlot>> = BTreeMap::new();
    for slot in slots {
        let cpu = cpu_for_xdp_queue(&slot)?;
        by_cpu.entry(cpu).or_default().push(slot);
    }
    Ok(by_cpu
        .into_iter()
        .map(|(cpu, slots)| QueueGroup { cpu, slots })
        .collect())
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
    while !start.load(Ordering::Acquire)
        && !stop.load(Ordering::Relaxed)
        && !shutdown_requested()
    {
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

fn validate_blast_flow_ports(local: SocketAddrV4, slots: &[XdpQueueSlot]) -> Result<(), BoxError> {
    if slots.is_empty() {
        return Err("no XDP queue slots discovered".into());
    }
    if local.port() == 0 {
        return Err("--local port must be non-zero with --all-queues".into());
    }
    let max_offset = slots
        .iter()
        .map(|slot| slot.flat_index.get())
        .max()
        .unwrap_or(0);
    if u32::from(local.port()) + max_offset > u32::from(u16::MAX) {
        return Err(format!(
            "--local port {} leaves too few UDP source ports for {} queue slots",
            local.port(),
            slots.len()
        )
        .into());
    }
    Ok(())
}

fn local_for_blast_slot(
    local: SocketAddrV4,
    flat_index: QueueId,
) -> Result<SocketAddrV4, BoxError> {
    let port = u32::from(local.port()) + flat_index.get();
    Ok(SocketAddrV4::new(*local.ip(), u16::try_from(port)?))
}
