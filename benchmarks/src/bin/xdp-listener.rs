use std::collections::BTreeMap;
use std::net::{SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use fast_socket_benchmarks::{
    Args, BoxError, Progress, RunLimit, XdpProgramMap, attach_xdp_programs_for_slots,
    install_shutdown_signal_handlers, parse_ipv4_udp, pin_current_thread_to_cpu, reflect_ipv4_udp,
    shutdown_requested, xdp_program_for_slot,
};
use fast_socket_rs::{
    IfIndex, IpPacketSocket, IpPacketTransmit, PacketBuffer, PacketBufferMut, QueueId, RecvBatch,
    TxSlot,
};
use fast_socket_xdp_rs::{
    BusyPollXdpIpPacketSocket, RouteSnapshot, XdpIpPacketSocketBuilder, XdpQueueSlot,
    cpu_for_xdp_queue, if_index_to_name, xdp_queue_slots_for_interface,
};

const USAGE: &str = "usage: xdp-listener <count|pong> (--ifindex N | --iface NAME) --bind IPv4:PORT [--count N] [--duration-ms N]";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Count,
    Pong,
}

struct QueueGroup {
    cpu: u32,
    slots: Vec<XdpQueueSlot>,
}

fn main() -> Result<(), BoxError> {
    install_shutdown_signal_handlers()?;
    let mut args = Args::new();
    let mode = match args.mode(USAGE)?.as_str() {
        "count" => Mode::Count,
        "pong" => Mode::Pong,
        other => return Err(format!("unknown mode {other}\n{USAGE}").into()),
    };
    let slots = queue_slots_from_args(&mut args)?;
    let bind = socket_addr_v4(args.required("--bind")?, "--bind")?;
    let limit = RunLimit::from_args(&mut args)?;
    args.finish()?;

    let programs = Arc::new(attach_xdp_programs_for_slots(&slots)?);
    let groups = queue_groups_by_cpu(slots)?;
    eprintln!(
        "xdp-listener: {} queue sockets coalesced onto {} CPU threads",
        groups.iter().map(|group| group.slots.len()).sum::<usize>(),
        groups.len()
    );

    let routes = RouteSnapshot::from_netlink(QueueId::new(0))?;
    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));
    let (error_tx, error_rx) = mpsc::channel::<String>();
    let mut handles = Vec::with_capacity(groups.len());

    for group in groups {
        let worker_routes = routes.clone();
        let worker_stop = Arc::clone(&stop);
        let worker_total = Arc::clone(&total);
        let worker_error_tx = error_tx.clone();
        let worker_programs = Arc::clone(&programs);
        handles.push(thread::spawn(move || {
            if let Err(error) = run_worker(
                mode,
                group.cpu,
                group.slots,
                worker_routes,
                worker_programs,
                bind.port(),
                worker_stop.clone(),
                worker_total,
            ) {
                let _ = worker_error_tx.send(format!("worker cpu {}: {error}", group.cpu));
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
    progress.finish(total.load(Relaxed));
    Ok(())
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

#[allow(clippy::too_many_arguments)]
fn run_worker(
    mode: Mode,
    cpu: u32,
    slots: Vec<XdpQueueSlot>,
    routes: RouteSnapshot,
    programs: Arc<XdpProgramMap>,
    bind_port: u16,
    stop: Arc<AtomicBool>,
    total: Arc<AtomicU64>,
) -> Result<(), BoxError> {
    pin_current_thread_to_cpu(cpu)?;

    let mut sockets = Vec::with_capacity(slots.len());
    for slot in slots {
        let program = xdp_program_for_slot(&programs, &slot)?;
        let socket = XdpIpPacketSocketBuilder::new(slot.ifindex, slot.queue)
            .bind_udp_port(bind_port)
            .attached_program(program.clone())
            .open_busy_poll_live()?;
        sockets.push((slot, socket));
    }

    let mut rx = RecvBatch::with_capacity(64);
    while !stop.load(Relaxed) && !shutdown_requested() {
        let mut made_progress = false;
        for (slot, socket) in &mut sockets {
            rx.clear();
            let received = socket.recv(&mut rx)?;
            if received == 0 {
                if mode == Mode::Pong {
                    socket.drain_tx_completions()?;
                }
                continue;
            }
            made_progress = true;
            match mode {
                Mode::Count => count_received(&mut rx, bind_port, &total),
                Mode::Pong => pong_received(socket, &routes, slot, bind_port, &total, &mut rx)?,
            }
        }
        if !made_progress {
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
) {
    for item in rx.drain() {
        if parse_ipv4_udp(item.packet.segments().next().unwrap_or_default())
            .is_some_and(|udp| udp.destination_port == bind_port)
        {
            total.fetch_add(1, Relaxed);
        }
    }
}

fn pong_received(
    socket: &mut BusyPollXdpIpPacketSocket,
    routes: &RouteSnapshot,
    slot: &XdpQueueSlot,
    bind_port: u16,
    total: &AtomicU64,
    rx: &mut RecvBatch<
        fast_socket_rs::IpPacketReceive<
            fast_socket_rs::IpPacketRxBuffer<BusyPollXdpIpPacketSocket>,
        >,
    >,
) -> Result<(), BoxError> {
    for mut item in rx.drain() {
        let Some(parsed) = parse_ipv4_udp(item.packet.segments().next().unwrap_or_default()) else {
            continue;
        };
        if parsed.destination_port != bind_port {
            continue;
        }
        if item.packet.len() > parsed.total_len {
            item.packet
                .trim_suffix(item.packet.len() - parsed.total_len)?;
        }
        let Some(destination) = reflect_ipv4_udp(item.packet.as_mut_slice()) else {
            continue;
        };
        let Some(egress) = routes.egress_v4_for_interface(destination, slot.ifindex, slot.queue)
        else {
            continue;
        };
        let mut tx = [TxSlot::Ready(IpPacketTransmit::new(
            item.packet.freeze(),
            egress,
        ))];
        if socket.send(&mut tx)? == 1 {
            total.fetch_add(1, Relaxed);
        }
    }
    socket.drain_tx_completions()?;
    Ok(())
}

fn join_workers(handles: Vec<thread::JoinHandle<()>>) -> Result<(), BoxError> {
    for handle in handles {
        if handle.join().is_err() {
            return Err("xdp-listener worker thread panicked".into());
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
