use std::collections::BTreeMap;
use std::net::SocketAddrV4;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use fast_socket_benchmarks::{
    BoxError, Progress, RunLimit, XdpProgramMap, attach_xdp_programs_for_slots,
    install_shutdown_signal_handlers, parse_ipv4_udp, pin_current_thread_to_cpu, reflect_ipv4_udp,
    shutdown_requested, xdp_program_for_slot,
};
use fast_socket_rs::{
    IfIndex, IpPacketSocket, IpPacketTransmit, PacketBuffer, PacketBufferMut, RecvBatch,
    TxSlot,
};
use fast_socket_xdp_rs::{
    BusyPollXdpIpPacketSocket, RouteSnapshot, XdpIpPacketSocketBuilder, XdpQueueSlot,
    cpu_for_xdp_queue, if_index_to_name, xdp_queue_slots_for_interface,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Mode {
    Count,
    Pong,
}

struct QueueGroup {
    cpu: u32,
    slots: Vec<XdpQueueSlot>,
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

    #[command(flatten)]
    limit: RunLimit,
}

fn main() -> Result<(), BoxError> {
    install_shutdown_signal_handlers()?;
    let cli = Cli::parse();
    let mode = cli.mode;
    let slots = queue_slots_from_cli(cli.ifindex, cli.iface)?;
    let bind = cli.bind;
    let limit = cli.limit;

    let programs = Arc::new(attach_xdp_programs_for_slots(&slots)?);
    let groups = queue_groups_by_cpu(slots)?;
    eprintln!(
        "xdp-listener: {} queue sockets coalesced onto {} CPU threads",
        groups.iter().map(|group| group.slots.len()).sum::<usize>(),
        groups.len()
    );

    let routes = RouteSnapshot::from_netlink()?;
    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));
    let dropped = Arc::new(AtomicU64::new(0));
    let (error_tx, error_rx) = mpsc::channel::<String>();
    let mut handles = Vec::with_capacity(groups.len());

    for group in groups {
        let worker_routes = routes.clone();
        let worker_stop = Arc::clone(&stop);
        let worker_total = Arc::clone(&total);
        let worker_dropped = Arc::clone(&dropped);
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
                worker_dropped,
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
    dropped: Arc<AtomicU64>,
) -> Result<(), BoxError> {
    pin_current_thread_to_cpu(cpu)?;

    let mut sockets = Vec::with_capacity(slots.len());
    for slot in slots {
        let program = xdp_program_for_slot(&programs, &slot)?;
        let socket = XdpIpPacketSocketBuilder::new(slot.ifindex, slot.queue)
            .bind_udp_port(bind_port)
            .attached_program(program.clone())
            .open_busy_poll()?;
        sockets.push((slot, socket));
    }

    let mut rx = RecvBatch::with_capacity(64);
    while !stop.load(Relaxed) && !shutdown_requested() {
        let mut delivered_this_pass = 0u64;
        for (slot, socket) in &mut sockets {
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
                Mode::Pong => {
                    pong_received(socket, &routes, slot, bind_port, &total, &dropped, &mut rx)?
                }
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

#[allow(clippy::too_many_arguments)]
fn pong_received(
    socket: &mut BusyPollXdpIpPacketSocket,
    routes: &RouteSnapshot,
    slot: &XdpQueueSlot,
    bind_port: u16,
    total: &AtomicU64,
    dropped: &AtomicU64,
    rx: &mut RecvBatch<
        fast_socket_rs::IpPacketReceive<
            fast_socket_rs::IpPacketRxBuffer<BusyPollXdpIpPacketSocket>,
        >,
    >,
) -> Result<u64, BoxError> {
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
        let Some(egress) = routes.egress_v4_for_interface(destination, slot.ifindex, slot.queue)
        else {
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

