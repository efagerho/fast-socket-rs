#[path = "../common.rs"]
mod common;

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket as StdUdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use clap::Parser;
use common::{
    BoxError, install_shutdown_signal_handlers, interface_ipv4_addr, payload, shutdown_requested,
    write_sequence,
};
use fast_socket_rs::{BusyPollDriver, PacketBufferMut, QueueId};
use fast_socket_udp_tile::{AcceptAllClassifier, TileError, UdpNetworkTile, UdpNetworkTileHandle};
use fast_socket_udp_tile_xdp::{Spin, UdpSocketSet, UdpTile, UdpTileHandle};
use fast_socket_xdp_rs::{
    InterfaceSelector, PortFilter, RouteSnapshot, XdpFactoryBuilder, XdpQueueLocalRouter,
    XdpRouteMonitor, XdpRouteMonitorHandle, XdpUdpAggregate, XdpUdpSocket,
};

const BATCH_SIZE: usize = 64;
const PAYLOAD_LEN: usize = 64;
const PROGRESS_INTERVAL: Duration = Duration::from_secs(1);

type XdpTile = UdpTile<MonitoredXdpAggregate, Spin, AcceptAllClassifier>;
type XdpTileHandle = UdpTileHandle<MonitoredXdpAggregate, Spin, AcceptAllClassifier>;
type XdpAggregate = XdpUdpAggregate<BusyPollDriver, XdpQueueLocalRouter>;
type XdpSocket = XdpUdpSocket<BusyPollDriver, XdpQueueLocalRouter>;

#[derive(Debug, Parser)]
struct Args {
    /// Device name to attach to.
    #[arg(long)]
    device: String,

    /// Target UDP endpoint as IPv4:PORT.
    #[arg(long)]
    target: SocketAddrV4,

    /// Number of XDP tile threads. All NIC queues are split into this many
    /// contiguous worker plans. Must divide the queue count.
    #[arg(long, default_value_t = 1)]
    threads: usize,

    /// Number of application producer lanes feeding each tile.
    #[arg(long, default_value_t = 1)]
    lane_count: usize,

    /// UDP payload length.
    #[arg(long, default_value_t = PAYLOAD_LEN)]
    payload_len: usize,
}

struct MonitoredXdpAggregate {
    aggregate: XdpAggregate,
    route_updates: Vec<XdpRouteMonitorHandle>,
}

impl UdpSocketSet for MonitoredXdpAggregate {
    type Socket = XdpSocket;

    fn poll_maintenance(&mut self) -> bool {
        let mut updates = 0usize;
        for (socket, route_update) in self
            .aggregate
            .members_mut()
            .iter_mut()
            .zip(self.route_updates.iter_mut())
        {
            updates += route_update.apply_updates(socket.routes_mut());
        }
        updates != 0
    }

    fn sockets_mut(&mut self) -> &mut [Self::Socket] {
        self.aggregate.members_mut()
    }

    fn can_transmit_from_any_socket(&self) -> bool {
        self.aggregate.members_share_umem()
    }
}

fn main() -> Result<(), BoxError> {
    install_shutdown_signal_handlers()?;
    let args = Args::parse();
    if args.threads == 0 {
        return Err("--threads must be at least 1".into());
    }
    if args.lane_count == 0 {
        return Err("--lane-count must be at least 1".into());
    }
    if args.payload_len == 0 {
        return Err("--payload-len must be at least 1".into());
    }

    run_tile_blast(
        &args.device,
        args.target,
        args.threads,
        args.lane_count,
        args.payload_len,
    )
}

fn run_tile_blast(
    device: &str,
    target: SocketAddrV4,
    threads: usize,
    lane_count: usize,
    payload_len: usize,
) -> Result<(), BoxError> {
    let local_ip = interface_ipv4_addr(device)?;
    let local = SocketAddrV4::new(local_ip, kernel_assigned_udp_port(local_ip)?);
    let routes = RouteSnapshot::from_netlink()?;
    let mut route_monitor = XdpRouteMonitor::new();
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

    let mut tiles = Vec::with_capacity(workers.len());
    let mut tile_handles = Vec::with_capacity(workers.len());
    for (tile_index, (plan, route_updates)) in workers.into_iter().enumerate() {
        let tile = Arc::new(XdpTile::new(
            move || MonitoredXdpAggregate {
                aggregate: plan
                    .open_udp_busy_poll(local)
                    .expect("failed to open XDP tile aggregate"),
                route_updates,
            },
            AcceptAllClassifier,
            lane_count,
        ));
        let handle = Arc::clone(&tile).start(tile_index)?;
        tiles.push(tile);
        tile_handles.push(Some(handle));
    }

    eprintln!(
        "tile-blast xdp: {} tile thread(s), {} producer lane(s), sending {}-byte UDP payloads from {local} to {target}",
        tiles.len(),
        lane_count,
        payload_len
    );

    run_producers(tiles, tile_handles, target.into(), lane_count, payload_len)
}

fn run_producers(
    tiles: Vec<Arc<XdpTile>>,
    mut tile_handles: Vec<Option<JoinHandle<Result<(), TileError>>>>,
    target: SocketAddr,
    lane_count: usize,
    payload_len: usize,
) -> Result<(), BoxError> {
    let stop = Arc::new(AtomicBool::new(false));
    let queued = Arc::new(AtomicU64::new(0));
    let dropped = Arc::new(AtomicU64::new(0));
    let next_sequence = Arc::new(AtomicU64::new(0));
    let (error_tx, error_rx) = mpsc::channel::<String>();
    let started = Instant::now();
    let mut producers = Vec::with_capacity(lane_count);

    for lane_index in 0..lane_count {
        let handles = tiles
            .iter()
            .enumerate()
            .map(|(tile_index, tile)| {
                Arc::clone(tile)
                    .lane_handle(lane_index)
                    .ok_or_else(|| format!("tile {tile_index} has no handle for lane {lane_index}"))
            })
            .collect::<Result<Vec<_>, String>>()?;
        let stop = Arc::clone(&stop);
        let queued = Arc::clone(&queued);
        let dropped = Arc::clone(&dropped);
        let next_sequence = Arc::clone(&next_sequence);
        let error_tx = error_tx.clone();
        producers.push(thread::spawn(move || {
            if let Err(error) = run_lane(
                lane_index,
                handles,
                target,
                payload_len,
                &stop,
                &queued,
                &dropped,
                &next_sequence,
            ) {
                stop.store(true, Ordering::Relaxed);
                let _ = error_tx.send(format!("producer lane {lane_index} failed: {error}"));
            }
        }));
    }
    drop(error_tx);

    let mut last_report = started;
    let mut last_queued = 0u64;
    while !shutdown_requested() && !stop.load(Ordering::Relaxed) {
        if let Ok(error) = error_rx.try_recv() {
            stop.store(true, Ordering::Relaxed);
            return Err(error.into());
        }
        check_tile_threads(&mut tile_handles, &stop)?;

        thread::sleep(Duration::from_millis(200));
        let now = Instant::now();
        if now.duration_since(last_report) >= PROGRESS_INTERVAL {
            let count = queued.load(Ordering::Relaxed);
            let interval = now.duration_since(last_report).as_secs_f64();
            let rate = (count - last_queued) as f64 / interval;
            let lane_drops = dropped.load(Ordering::Relaxed);
            let stats = sum_tile_stats(&tiles);
            eprintln!(
                "tile-blast: {count} packets queued ({rate:.0} packets/s), lane drops {lane_drops}, tile drops {}",
                stats.classifier_drops + stats.rx_queue_drops + stats.tx_drops
            );
            last_report = now;
            last_queued = count;
        }
    }

    stop.store(true, Ordering::Relaxed);
    for producer in producers {
        if producer.join().is_err() {
            return Err("tile-blast producer thread panicked".into());
        }
    }

    let count = queued.load(Ordering::Relaxed);
    let elapsed = started.elapsed();
    let rate = if elapsed.is_zero() {
        0.0
    } else {
        count as f64 / elapsed.as_secs_f64()
    };
    println!("tile-blast: {count} packets queued in {elapsed:?} ({rate:.0} packets/s)");

    let lane_drops = dropped.load(Ordering::Relaxed);
    if lane_drops > 0 {
        eprintln!("tile-blast: dropped {lane_drops} packets to full lane TX queues");
    }
    let stats = sum_tile_stats(&tiles);
    if stats.classifier_drops + stats.rx_queue_drops + stats.tx_drops > 0 {
        eprintln!("tile-blast: tile stats: {stats:?}");
    }
    Ok(())
}

fn run_lane(
    lane_index: usize,
    mut handles: Vec<XdpTileHandle>,
    target: SocketAddr,
    payload_len: usize,
    stop: &AtomicBool,
    queued: &AtomicU64,
    dropped: &AtomicU64,
    next_sequence: &AtomicU64,
) -> Result<(), String> {
    debug_assert!(
        handles
            .iter()
            .all(|handle| handle.lane_index() == lane_index)
    );
    let mut payload_bytes = payload(payload_len);
    let mut tx_buffers = Vec::with_capacity(BATCH_SIZE);
    let mut local_queued = 0u64;
    let mut local_dropped = 0u64;

    while !stop.load(Ordering::Relaxed) && !shutdown_requested() {
        let mut progressed = false;
        for handle in &mut handles {
            drain_lane_rx(handle);
            tx_buffers.clear();
            let allocated = handle.alloc_tx_buffers(BATCH_SIZE, &mut tx_buffers);
            if allocated == 0 {
                continue;
            }
            progressed = true;
            let base_sequence = next_sequence.fetch_add(allocated as u64, Ordering::Relaxed);
            let mut batch = handle.alloc_tx_batch();

            for (offset, mut buffer) in tx_buffers.drain(..).enumerate() {
                write_sequence(&mut payload_bytes, base_sequence + offset as u64);
                buffer
                    .extend_from_slice(&payload_bytes)
                    .map_err(|error| error.to_string())?;
                batch.push(buffer.freeze(target));
            }

            let batch_len = batch.len() as u64;
            match handle.push_tx_batch(batch) {
                Ok(()) => local_queued += batch_len,
                Err(batch) => {
                    local_dropped += batch.len() as u64;
                    handle.recycle_tx_batch(batch);
                }
            }
        }

        if local_queued >= 1024 {
            queued.fetch_add(local_queued, Ordering::Relaxed);
            local_queued = 0;
        }
        if local_dropped >= 1024 {
            dropped.fetch_add(local_dropped, Ordering::Relaxed);
            local_dropped = 0;
        }
        if !progressed {
            std::hint::spin_loop();
        }
    }

    if local_queued > 0 {
        queued.fetch_add(local_queued, Ordering::Relaxed);
    }
    if local_dropped > 0 {
        dropped.fetch_add(local_dropped, Ordering::Relaxed);
    }
    Ok(())
}

fn drain_lane_rx(handle: &XdpTileHandle) {
    while let Some(batch) = handle.pop_rx_batch() {
        handle.recycle_rx_batch(batch);
    }
}

fn check_tile_threads(
    tile_handles: &mut [Option<JoinHandle<Result<(), TileError>>>],
    stop: &AtomicBool,
) -> Result<(), BoxError> {
    for (index, handle) in tile_handles.iter_mut().enumerate() {
        if !handle.as_ref().is_some_and(|handle| handle.is_finished()) {
            continue;
        }
        stop.store(true, Ordering::Relaxed);
        let handle = handle.take().expect("finished handle is present");
        match handle.join() {
            Ok(Ok(())) => return Err(format!("tile thread {index} exited unexpectedly").into()),
            Ok(Err(error)) => return Err(format!("tile thread {index} failed: {error}").into()),
            Err(_) => return Err(format!("tile thread {index} panicked").into()),
        }
    }
    Ok(())
}

fn sum_tile_stats(tiles: &[Arc<XdpTile>]) -> fast_socket_udp_tile::TileStats {
    let mut total = fast_socket_udp_tile::TileStats::default();
    for tile in tiles {
        let stats = tile.stats();
        total.classifier_drops += stats.classifier_drops;
        total.rx_queue_drops += stats.rx_queue_drops;
        total.tx_drops += stats.tx_drops;
    }
    total
}

fn kernel_assigned_udp_port(local_ip: Ipv4Addr) -> Result<u16, BoxError> {
    let probe = StdUdpSocket::bind(SocketAddrV4::new(local_ip, 0))?;
    let port = probe.local_addr()?.port();
    drop(probe);
    Ok(port)
}
