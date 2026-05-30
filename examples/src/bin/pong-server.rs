#[path = "../common.rs"]
mod common;

use std::net::SocketAddrV4;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use clap::Parser;
use fast_socket_rs::{
    PacketBufferMut, QueueId, RecvBatch, TxSlot, UdpReceive, UdpRecvMeta, UdpRxBuffer,
    UdpSocket as FastUdpSocket, UdpTransmit, UdpTxBuffer,
};

use fast_socket_xdp_rs::{
    BusyPollXdpUdpSocket, InterfaceSelector, PortFilter, RouteSnapshot, XdpFactoryBuilder,
    XdpRouteMonitor, XdpRouteMonitorHandle,
};

use common::{
    BoxError, Mode, Progress, install_shutdown_signal_handlers, interface_ipv4_addr,
    open_os_udp_socket, pin_current_thread_to_cpu, queue_plan, shutdown_requested,
};

const PAYLOAD_LEN: usize = 64;
const BATCH_SIZE: usize = 64;

#[derive(Debug, Parser)]
struct Args {
    /// Device name whose NIC queues should own the sockets.
    #[arg(long)]
    device: String,

    /// Expected peer endpoint as IP:PORT. The server binds the device IP with
    /// this port (so the peer can reach it) and, in XDP mode, uses the peer IP
    /// for queue-local egress resolution. UDP source addresses on incoming
    /// pings are echoed back regardless.
    #[arg(long)]
    target: SocketAddrV4,

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
        Mode::Os => run_os(args.device, args.target),
        Mode::Xdp => run_xdp(args.device, args.target, args.threads),
    }
}

fn pong_server<S>(socket: &mut S, stop: &AtomicBool, reflected: &AtomicU64) -> Result<(), BoxError>
where
    S: FastUdpSocket<RecvMeta = UdpRecvMeta>,
    UdpRxBuffer<S>: PacketBufferMut<Frozen = UdpTxBuffer<S>>,
{
    let mut rx: RecvBatch<UdpReceive<UdpRxBuffer<S>, UdpRecvMeta>> =
        RecvBatch::with_capacity(BATCH_SIZE);
    let mut tx: Vec<TxSlot<UdpTransmit<UdpTxBuffer<S>>>> = Vec::with_capacity(BATCH_SIZE);

    while !stop.load(Relaxed) && !shutdown_requested() {
        rx.clear();
        if socket.recv(&mut rx)? == 0 {
            socket.drain_tx_completions()?;
            thread::sleep(Duration::from_micros(50));
            continue;
        }

        tx.clear();
        for item in rx.drain() {
            tx.push(TxSlot::Ready(UdpTransmit::new(
                item.packet.freeze(),
                item.meta.source,
            )));
        }

        reflected.fetch_add(send_all(socket, &mut tx)? as u64, Relaxed);
        socket.drain_tx_completions()?;
    }

    Ok(())
}

fn run_os(device: String, target: SocketAddrV4) -> Result<(), BoxError> {
    let plans = queue_plan(&device)?;
    let bind = SocketAddrV4::new(interface_ipv4_addr(&device)?, target.port());
    eprintln!(
        "pong-server os: {} SO_REUSEPORT sockets bound to {} with SO_INCOMING_CPU",
        plans.len(),
        bind
    );

    run_workers(
        "pong-server os",
        plans,
        |plan| plan.cpu,
        move |plan, stop, total| {
            pin_current_thread_to_cpu(plan.cpu)?;
            let mut socket = open_os_udp_socket(
                &device,
                bind,
                plan.cpu,
                QueueId::new(plan.slot.flat_index.get()),
                PAYLOAD_LEN,
            )?;
            pong_server(&mut socket, &stop, &total)
        },
    )
}

fn run_xdp(device: String, target: SocketAddrV4, threads: usize) -> Result<(), BoxError> {
    let local = SocketAddrV4::new(interface_ipv4_addr(&device)?, target.port());
    let routes = RouteSnapshot::from_netlink()?;
    let mut route_monitor = XdpRouteMonitor::new();
    // Phase 1: discover queues, attach the program, partition into `threads`
    // worker plans (one aggregate socket each over queues/threads queues).
    let factory = XdpFactoryBuilder::new(InterfaceSelector::Name(device))?
        .threads(threads)
        .port_filter(PortFilter::UdpPorts(vec![target.port()]))
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
        "pong-server xdp: {} aggregate socket(s) / thread(s) bound to {} with egress toward {}",
        workers.len(),
        local,
        target.ip()
    );

    run_workers(
        "pong-server xdp",
        workers,
        |(plan, _)| plan.cpu(),
        move |(plan, mut route_updates), stop, total| {
            // Pins to plan.cpu() and opens this worker's aggregate.
            let mut aggregate = plan.open_udp_busy_poll(local)?;
            pong_aggregate(&mut aggregate, &mut route_updates, &stop, &total)
        },
    )
}

/// Pongs across every member of an aggregate, round-robin. Reflection leaves on
/// the queue a frame arrived on (each member owns its shared-UMEM frame slice).
fn pong_aggregate(
    aggregate: &mut fast_socket_xdp_rs::XdpUdpAggregate<
        fast_socket_rs::BusyPollDriver,
        fast_socket_xdp_rs::XdpQueueLocalRouter,
    >,
    route_updates: &mut [XdpRouteMonitorHandle],
    stop: &AtomicBool,
    reflected: &AtomicU64,
) -> Result<(), BoxError> {
    let mut rx: RecvBatch<UdpReceive<UdpRxBuffer<BusyPollXdpUdpSocket>, UdpRecvMeta>> =
        RecvBatch::with_capacity(BATCH_SIZE);
    let mut tx: Vec<TxSlot<UdpTransmit<UdpTxBuffer<BusyPollXdpUdpSocket>>>> =
        Vec::with_capacity(BATCH_SIZE);
    debug_assert_eq!(route_updates.len(), aggregate.len());

    while !stop.load(Relaxed) && !shutdown_requested() {
        let mut progress = 0usize;
        for (socket, route_update) in aggregate
            .members_mut()
            .iter_mut()
            .zip(route_updates.iter_mut())
        {
            route_update.apply_updates(socket.routes_mut());
            rx.clear();
            if socket.recv(&mut rx)? == 0 {
                socket.drain_tx_completions()?;
                continue;
            }
            tx.clear();
            for item in rx.drain() {
                tx.push(TxSlot::Ready(UdpTransmit::new(
                    item.packet.freeze(),
                    item.meta.source,
                )));
            }
            reflected.fetch_add(send_all(socket, &mut tx)? as u64, Relaxed);
            socket.drain_tx_completions()?;
            progress += 1;
        }
        if progress == 0 {
            thread::sleep(Duration::from_micros(50));
        }
    }
    Ok(())
}

fn run_workers<P, C, F>(
    name: &'static str,
    plans: Vec<P>,
    cpu_of: C,
    run: F,
) -> Result<(), BoxError>
where
    P: Send + 'static,
    C: Fn(&P) -> u32,
    F: Fn(P, Arc<AtomicBool>, Arc<AtomicU64>) -> Result<(), BoxError> + Send + Sync + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));
    let run = Arc::new(run);
    let (error_tx, error_rx) = mpsc::channel::<String>();
    let mut handles = Vec::with_capacity(plans.len());

    for plan in plans {
        let cpu = cpu_of(&plan);
        let worker_stop = Arc::clone(&stop);
        let worker_total = Arc::clone(&total);
        let worker_error_tx = error_tx.clone();
        let worker_run = Arc::clone(&run);
        handles.push(thread::spawn(move || {
            if let Err(error) = worker_run(plan, worker_stop.clone(), worker_total) {
                let _ = worker_error_tx.send(format!("worker cpu {cpu}: {error}"));
                worker_stop.store(true, Relaxed);
            }
        }));
    }
    drop(error_tx);

    let mut progress = Progress::new(name);
    while !shutdown_requested() && !stop.load(Relaxed) {
        if let Ok(error) = error_rx.try_recv() {
            stop.store(true, Relaxed);
            join_workers(handles)?;
            return Err(error.into());
        }
        progress.tick(total.load(Relaxed));
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

fn send_all<S>(
    socket: &mut S,
    batch: &mut [TxSlot<UdpTransmit<UdpTxBuffer<S>>>],
) -> Result<usize, BoxError>
where
    S: FastUdpSocket,
{
    let mut accepted = 0;
    while accepted < batch.len() {
        match socket.send(&mut batch[accepted..]) {
            Ok(0) => {
                socket.drain_tx_completions()?;
                std::hint::spin_loop();
            }
            Ok(n) => accepted += n,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(accepted)
}

fn join_workers(handles: Vec<thread::JoinHandle<()>>) -> Result<(), BoxError> {
    for handle in handles {
        if handle.join().is_err() {
            return Err("pong-server worker thread panicked".into());
        }
    }
    Ok(())
}
