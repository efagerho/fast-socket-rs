use std::collections::BTreeMap;
use std::net::{SocketAddr, SocketAddrV4};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use fast_socket_benchmarks::{
    Args, BoxError, Progress, RunLimit, XdpProgramMap, attach_xdp_programs_for_slots,
    install_shutdown_signal_handlers, payload, pin_current_thread_to_cpu, shutdown_requested,
    write_sequence, xdp_program_for_slot,
};
use fast_socket_rs::{
    BufferPool, IfIndex, PacketBufferMut, QueueId, RecvBatch, TxSlot, UdpSocket, UdpTransmit,
};
use fast_socket_xdp_rs::{
    BusyPollXdpUdpSocket, RouteSnapshot, XdpIpPacketSocketBuilder, XdpPacketBuf, XdpPacketBufMut,
    XdpProgramHandle, XdpQueueSlot, XdpUdpSocket, cpu_for_xdp_queue, if_index_to_name,
    resolve_xdp_queue_slot, xdp_queue_slots_for_interface,
};

const USAGE: &str = "usage: xdp-sender <blast|ping> (--ifindex N | --iface NAME) [--queue N | --all-queues] --local IPv4:PORT --dest IPv4:PORT [--payload-len N] [--count N] [--duration-ms N] [--rate PPS]";
const DEFAULT_PING_RATE: u64 = 1_000;
const BLAST_BATCH_SIZE: usize = 64;
const BLAST_COUNTER_FLUSH_PACKETS: u64 = BLAST_BATCH_SIZE as u64;

struct QueueGroup {
    cpu: u32,
    slots: Vec<XdpQueueSlot>,
}

#[repr(align(64))]
#[derive(Default)]
struct WorkerCounters {
    sent: AtomicU64,
    received: AtomicU64,
}

fn main() -> Result<(), BoxError> {
    install_shutdown_signal_handlers()?;
    let mut args = Args::new();
    let mode = args.mode(USAGE)?;
    let queue = args
        .take("--queue")
        .map(|value| value.parse::<u32>().map(QueueId::new))
        .transpose()?;
    let all_queues = args.flag("--all-queues");
    let local = socket_addr_v4(args.required("--local")?, "--local")?;
    let dest = socket_addr_v4(args.required("--dest")?, "--dest")?;
    let payload_len = args.optional("--payload-len", 64usize)?;
    let limit = RunLimit::from_args(&mut args)?;
    let rate = args.optional("--rate", DEFAULT_PING_RATE)?;

    match mode.as_str() {
        "blast" => {
            if all_queues {
                if queue.is_some() {
                    return Err("use only one of --queue or --all-queues".into());
                }
                let slots = queue_slots_from_args(&mut args)?;
                args.finish()?;
                blast_all(slots, local, dest, payload_len, limit)
            } else {
                let queue = queue.unwrap_or_else(|| QueueId::new(0));
                let slot = queue_slot_from_args(&mut args, queue)?;
                args.finish()?;
                let routes = RouteSnapshot::from_netlink(slot.queue)?;
                let mut socket = open_udp_socket(&slot, &routes, local, dest, None)?;
                blast(&mut socket, dest.into(), payload_len, limit)
            }
        }
        "ping" => {
            if queue.is_some() {
                return Err(
                    "ping mode listens on all queues; --queue is only valid with blast".into(),
                );
            }
            let slots = queue_slots_from_args(&mut args)?;
            args.finish()?;
            ping(slots, local, dest, payload_len, limit, rate)
        }
        _ => Err(format!("unknown mode {mode}\n{USAGE}").into()),
    }
}

fn queue_slot_from_args(args: &mut Args, queue: QueueId) -> Result<XdpQueueSlot, BoxError> {
    let ifindex = args
        .take("--ifindex")
        .map(|value| value.parse::<u32>())
        .transpose()?
        .map(IfIndex::new);
    let iface = args.take("--iface");

    match (ifindex, iface) {
        (Some(ifindex), None) => Ok(XdpQueueSlot {
            iface: format!("if{}", ifindex.get()),
            ifindex,
            queue,
            flat_index: queue,
        }),
        (None, Some(iface)) => Ok(resolve_xdp_queue_slot(&iface, queue)?),
        (Some(_), Some(_)) => Err("use only one of --ifindex or --iface".into()),
        (None, None) => Err("missing --ifindex N or --iface NAME".into()),
    }
}

fn queue_slots_from_args(args: &mut Args) -> Result<Vec<XdpQueueSlot>, BoxError> {
    let ifindex = args
        .take("--ifindex")
        .map(|value| value.parse::<u32>())
        .transpose()?
        .map(IfIndex::new);
    let iface = args.take("--iface");

    match (ifindex, iface) {
        (Some(ifindex), None) => {
            let iface = if_index_to_name(ifindex)?;
            Ok(xdp_queue_slots_for_interface(&iface)?)
        }
        (None, Some(iface)) => Ok(xdp_queue_slots_for_interface(&iface)?),
        (Some(_), Some(_)) => Err("use only one of --ifindex or --iface".into()),
        (None, None) => Err("missing --ifindex N or --iface NAME".into()),
    }
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
    let mut builder = XdpIpPacketSocketBuilder::new(slot.ifindex, slot.queue)
        .mtu(egress.mtu as usize)
        .route_snapshot(routes.clone())
        .bind_udp_port(local.port());
    if let Some(program) = program {
        builder = builder.attached_program(program.clone());
    }
    let ip_socket = builder.open_busy_poll_live()?;
    Ok(XdpUdpSocket::new(ip_socket, local))
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
    let mut count = 0;
    let mut bytes = payload(payload_len);
    while limit.keep_running(count, started) && !shutdown_requested() {
        write_sequence(&mut bytes, count);
        let Some(mut packet) = socket.tx_pool_mut().allocate() else {
            socket.drain_tx_completions()?;
            std::hint::spin_loop();
            continue;
        };
        packet.extend_from_slice(&bytes)?;
        let mut batch = [TxSlot::Ready(UdpTransmit::new(packet.freeze(), dest))];
        match socket.send(&mut batch) {
            Ok(1) => count += 1,
            Ok(_) => std::hint::spin_loop(),
            Err(error) => return Err(error.into()),
        }
        socket.drain_tx_completions()?;
        progress.tick(count);
    }
    progress.finish(count);
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

    let routes = RouteSnapshot::from_netlink(QueueId::new(0))?;
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
                worker_stop.store(true, Relaxed);
            }
        }));
    }
    drop(error_tx);
    drop(ready_tx);

    if let Err(error) = wait_for_workers_ready(worker_count, &ready_rx, &error_rx, &stop) {
        stop.store(true, Relaxed);
        join_workers(handles)?;
        return Err(error);
    }

    let started = Instant::now();
    start.store(true, Relaxed);
    let stats_stop = Arc::clone(&stop);
    let stats_counters = counters.clone();
    let stats = thread::spawn(move || blast_stats_loop(stats_counters, stats_stop));

    loop {
        if let Ok(error) = error_rx.try_recv() {
            stop.store(true, Relaxed);
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

    stop.store(true, Relaxed);
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

fn ping(
    slots: Vec<XdpQueueSlot>,
    local: SocketAddrV4,
    dest: SocketAddrV4,
    payload_len: usize,
    limit: RunLimit,
    rate: u64,
) -> Result<(), BoxError> {
    if rate == 0 {
        return Err("--rate must be greater than 0".into());
    }

    let programs = Arc::new(attach_xdp_programs_for_slots(&slots)?);
    let groups = queue_groups_by_cpu(slots)?;
    let total_slots = groups.iter().map(|group| group.slots.len()).sum::<usize>();
    eprintln!(
        "xdp-sender ping: {total_slots} queue sockets coalesced onto {} CPU threads at {rate} pings/s",
        groups.len()
    );

    let routes = RouteSnapshot::from_netlink(QueueId::new(0))?;
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
        let worker_rate = rate as f64 * group.slots.len() as f64 / total_slots as f64;
        handles.push(thread::spawn(move || {
            if let Err(error) = run_ping_worker(
                group.cpu,
                group.slots,
                worker_routes,
                worker_programs,
                local,
                dest,
                payload_len,
                worker_rate,
                worker_stop.clone(),
                worker_start,
                worker_ready_tx,
                worker_counters,
            ) {
                let _ = worker_error_tx.send(format!("worker cpu {}: {error}", group.cpu));
                worker_stop.store(true, Relaxed);
            }
        }));
    }
    drop(error_tx);
    drop(ready_tx);

    if let Err(error) = wait_for_workers_ready(worker_count, &ready_rx, &error_rx, &stop) {
        stop.store(true, Relaxed);
        join_workers(handles)?;
        return Err(error);
    }

    let started = Instant::now();
    start.store(true, Relaxed);
    let stats_stop = Arc::clone(&stop);
    let stats_counters = counters.clone();
    let stats = thread::spawn(move || stats_loop(stats_counters, stats_stop));

    loop {
        if let Ok(error) = error_rx.try_recv() {
            stop.store(true, Relaxed);
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

    stop.store(true, Relaxed);
    join_workers(handles)?;
    let _ = stats.join();
    if let Ok(error) = error_rx.try_recv() {
        return Err(error.into());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_ping_worker(
    cpu: u32,
    slots: Vec<XdpQueueSlot>,
    routes: RouteSnapshot,
    programs: Arc<XdpProgramMap>,
    local: SocketAddrV4,
    dest: SocketAddrV4,
    payload_len: usize,
    rate: f64,
    stop: Arc<AtomicBool>,
    start: Arc<AtomicBool>,
    ready_tx: mpsc::Sender<u32>,
    counters: Arc<WorkerCounters>,
) -> Result<(), BoxError> {
    pin_current_thread_to_cpu(cpu)?;

    let mut sockets = Vec::with_capacity(slots.len());
    for slot in slots {
        let program = xdp_program_for_slot(&programs, &slot)?;
        let socket = open_udp_socket(&slot, &routes, local, dest, Some(program))?;
        sockets.push((slot, local, socket));
    }

    signal_worker_ready(&ready_tx, cpu)?;
    if !wait_for_worker_start(&start, &stop) {
        return Ok(());
    }

    let interval = Duration::from_secs_f64(1.0 / rate);
    let mut next_send = Instant::now();
    let mut next_socket = 0usize;
    let mut sequence = 0u64;
    let mut bytes = payload(payload_len);
    let mut rx = RecvBatch::with_capacity(64);

    while !stop.load(Relaxed) && !shutdown_requested() {
        let now = Instant::now();
        while next_send <= now && !stop.load(Relaxed) && !shutdown_requested() {
            let (slot, local, socket) = &mut sockets[next_socket];
            let sent = send_packet(socket, dest.into(), &mut bytes, sequence).map_err(|error| {
                format!(
                    "{} queue {} flat {} local {}: {error}",
                    slot.iface,
                    slot.queue.get(),
                    slot.flat_index.get(),
                    local
                )
            })?;
            if sent {
                counters.sent.fetch_add(1, Relaxed);
                sequence = sequence.wrapping_add(1);
            }
            next_socket = (next_socket + 1) % sockets.len();
            next_send += interval;
        }

        let mut made_progress = false;
        for (slot, local, socket) in &mut sockets {
            rx.clear();
            if socket.recv(&mut rx).map_err(|error| {
                format!(
                    "{} queue {} flat {} local {} recv: {error}",
                    slot.iface,
                    slot.queue.get(),
                    slot.flat_index.get(),
                    local
                )
            })? == 0
            {
                socket.drain_tx_completions().map_err(|error| {
                    format!(
                        "{} queue {} flat {} local {} drain: {error}",
                        slot.iface,
                        slot.queue.get(),
                        slot.flat_index.get(),
                        local
                    )
                })?;
                continue;
            }
            made_progress = true;
            for item in rx.drain() {
                if item.meta.source == SocketAddr::V4(dest) {
                    counters.received.fetch_add(1, Relaxed);
                }
            }
            socket.drain_tx_completions().map_err(|error| {
                format!(
                    "{} queue {} flat {} local {} drain: {error}",
                    slot.iface,
                    slot.queue.get(),
                    slot.flat_index.get(),
                    local
                )
            })?;
        }

        if !made_progress && Instant::now() < next_send {
            thread::sleep(Duration::from_micros(50));
        }
    }

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
    let mut pending_sent = 0u64;
    const SHUTDOWN_CHECK_MASK: u32 = 0xff;
    let mut shutdown_check_counter: u32 = 0;

    while !stop.load(Relaxed) {
        let (slot, local, socket) = &mut sockets[next_socket];
        let sent = send_packet_batch(
            socket,
            dest.into(),
            &mut bytes,
            &mut sequence,
            &mut tx_buffers,
            &mut batch,
        )
        .map_err(|error| {
            format!(
                "{} queue {} flat {} local {}: {error}",
                slot.iface,
                slot.queue.get(),
                slot.flat_index.get(),
                local
            )
        })?;
        if sent > 0 {
            pending_sent += sent as u64;
            flush_blast_sent(&counters, &mut pending_sent, false);
        }
        next_socket = (next_socket + 1) % sockets.len();
        shutdown_check_counter = shutdown_check_counter.wrapping_add(1);
        if shutdown_check_counter & SHUTDOWN_CHECK_MASK == 0 && shutdown_requested() {
            break;
        }
    }

    flush_blast_sent(&counters, &mut pending_sent, true);
    Ok(())
}

fn send_packet_batch(
    socket: &mut BusyPollXdpUdpSocket,
    dest: SocketAddr,
    bytes: &mut [u8],
    sequence: &mut u64,
    tx_buffers: &mut Vec<XdpPacketBufMut>,
    batch: &mut Vec<TxSlot<UdpTransmit<XdpPacketBuf>>>,
) -> Result<usize, BoxError> {
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
        return Ok(0);
    }

    let accepted = socket.send(batch.as_mut_slice())?;
    if accepted < batch.len() {
        *sequence = sequence.wrapping_sub((batch.len() - accepted) as u64);
        socket.drain_tx_completions()?;
    }
    Ok(accepted)
}

fn flush_blast_sent(counters: &WorkerCounters, pending_sent: &mut u64, force: bool) {
    if *pending_sent >= BLAST_COUNTER_FLUSH_PACKETS || (force && *pending_sent > 0) {
        counters.sent.fetch_add(*pending_sent, Relaxed);
        *pending_sent = 0;
    }
}

fn send_packet(
    socket: &mut BusyPollXdpUdpSocket,
    dest: SocketAddr,
    bytes: &mut [u8],
    sequence: u64,
) -> Result<bool, BoxError> {
    write_sequence(bytes, sequence);
    let mut packet = match socket.tx_pool_mut().allocate() {
        Some(packet) => packet,
        None => {
            socket.drain_tx_completions()?;
            return Ok(false);
        }
    };
    packet.extend_from_slice(bytes)?;
    let mut batch = [TxSlot::Ready(UdpTransmit::new(packet.freeze(), dest))];
    let accepted = socket.send(&mut batch)?;
    socket.drain_tx_completions()?;
    Ok(accepted == 1)
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

fn stats_loop(counters: Vec<Arc<WorkerCounters>>, stop: Arc<AtomicBool>) {
    let mut prev_sent = 0u64;
    let mut prev_received = 0u64;
    while !stop.load(Relaxed) && !shutdown_requested() {
        thread::sleep(Duration::from_secs(1));
        if stop.load(Relaxed) || shutdown_requested() {
            break;
        }
        let sent = sum_sent(&counters);
        let received = sum_received(&counters);
        eprintln!(
            "xdp-sender ping: pings_sent/s={} pongs_received/s={} total_pings_sent={} total_pongs_received={}",
            sent.saturating_sub(prev_sent),
            received.saturating_sub(prev_received),
            sent,
            received
        );
        prev_sent = sent;
        prev_received = received;
    }
}

fn blast_stats_loop(counters: Vec<Arc<WorkerCounters>>, stop: Arc<AtomicBool>) {
    let mut prev_sent = 0u64;
    while !stop.load(Relaxed) && !shutdown_requested() {
        thread::sleep(Duration::from_secs(1));
        if stop.load(Relaxed) || shutdown_requested() {
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
            stop.store(true, Relaxed);
            return Err(error.into());
        }

        match ready_rx.recv_timeout(Duration::from_millis(100)) {
            Ok(_) => ready += 1,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if shutdown_requested() {
                    stop.store(true, Relaxed);
                    return Err("shutdown requested before XDP workers were ready".into());
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                stop.store(true, Relaxed);
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
    while !start.load(Relaxed) && !stop.load(Relaxed) && !shutdown_requested() {
        thread::yield_now();
    }
    start.load(Relaxed) && !stop.load(Relaxed) && !shutdown_requested()
}

fn sum_sent(counters: &[Arc<WorkerCounters>]) -> u64 {
    counters
        .iter()
        .map(|counter| counter.sent.load(Relaxed))
        .sum()
}

fn sum_received(counters: &[Arc<WorkerCounters>]) -> u64 {
    counters
        .iter()
        .map(|counter| counter.received.load(Relaxed))
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

fn socket_addr_v4(addr: SocketAddr, name: &str) -> Result<SocketAddrV4, BoxError> {
    match addr {
        SocketAddr::V4(addr) => Ok(addr),
        SocketAddr::V6(_) => {
            Err(format!("{name} must be IPv4 for the first XDP benchmark path").into())
        }
    }
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
