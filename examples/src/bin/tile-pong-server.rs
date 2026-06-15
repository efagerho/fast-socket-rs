#[path = "../common.rs"]
mod common;

use std::net::SocketAddrV4;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use clap::Parser;
use common::{BoxError, install_shutdown_signal_handlers, interface_ipv4_addr, shutdown_requested};
use fast_socket_rs::{BusyPollDriver, QueueId};
use fast_socket_udp_tile::{SourceAddrClassifier, TileError, UdpNetworkTile, UdpNetworkTileHandle};
use fast_socket_udp_tile_xdp::{Spin, UdpSocketSet, UdpTile, UdpTileHandle};
use fast_socket_xdp_rs::{
    InterfaceSelector, PortFilter, RouteSnapshot, XdpFactoryBuilder, XdpQueueLocalRouter,
    XdpRouteMonitor, XdpRouteMonitorHandle, XdpUdpAggregate, XdpUdpSocket,
};

const PROGRESS_INTERVAL: Duration = Duration::from_secs(1);

type XdpTile = UdpTile<MonitoredXdpAggregate, Spin, SourceAddrClassifier>;
type XdpTileHandle = UdpTileHandle<MonitoredXdpAggregate, Spin, SourceAddrClassifier>;
type XdpAggregate = XdpUdpAggregate<BusyPollDriver, XdpQueueLocalRouter>;
type XdpSocket = XdpUdpSocket<BusyPollDriver, XdpQueueLocalRouter>;

#[derive(Debug, Parser)]
struct Args {
    /// Device name to attach to.
    #[arg(long)]
    device: String,

    /// Expected peer endpoint as IPv4:PORT. The server binds the device IP with
    /// this port and reflects incoming UDP payloads back to their source.
    #[arg(long)]
    target: SocketAddrV4,

    /// Number of XDP tile threads. All NIC queues are split into this many
    /// contiguous worker plans. Must divide the queue count.
    #[arg(long, default_value_t = 1)]
    threads: usize,

    /// Number of application lanes receiving batches from each tile.
    #[arg(long, default_value_t = 1)]
    lane_count: usize,
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

    run_tile_pong_server(&args.device, args.target, args.threads, args.lane_count)
}

fn run_tile_pong_server(
    device: &str,
    target: SocketAddrV4,
    threads: usize,
    lane_count: usize,
) -> Result<(), BoxError> {
    let local = SocketAddrV4::new(interface_ipv4_addr(device)?, target.port());
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
            SourceAddrClassifier,
            lane_count,
        ));
        let handle = Arc::clone(&tile).start(tile_index)?;
        tiles.push(tile);
        tile_handles.push(Some(handle));
    }

    eprintln!(
        "tile-pong-server xdp: {} tile thread(s), {} lane(s), bound to {} with egress toward {}",
        tiles.len(),
        lane_count,
        local,
        target.ip()
    );

    run_lanes(tiles, tile_handles, lane_count)
}

fn run_lanes(
    tiles: Vec<Arc<XdpTile>>,
    mut tile_handles: Vec<Option<JoinHandle<Result<(), TileError>>>>,
    lane_count: usize,
) -> Result<(), BoxError> {
    let stop = Arc::new(AtomicBool::new(false));
    let reflected = Arc::new(AtomicU64::new(0));
    let dropped = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let mut lanes = Vec::with_capacity(lane_count);

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
        let reflected = Arc::clone(&reflected);
        let dropped = Arc::clone(&dropped);
        lanes.push(thread::spawn(move || {
            run_lane(lane_index, handles, &stop, &reflected, &dropped);
        }));
    }

    let mut last_report = started;
    let mut last_reflected = 0u64;
    while !shutdown_requested() && !stop.load(Ordering::Relaxed) {
        if let Err(error) = check_tile_threads(&mut tile_handles, &stop) {
            stop.store(true, Ordering::Relaxed);
            join_lanes(lanes)?;
            return Err(error);
        }

        thread::sleep(Duration::from_millis(200));
        let now = Instant::now();
        if now.duration_since(last_report) >= PROGRESS_INTERVAL {
            let count = reflected.load(Ordering::Relaxed);
            let interval = now.duration_since(last_report).as_secs_f64();
            let rate = (count - last_reflected) as f64 / interval;
            let lane_drops = dropped.load(Ordering::Relaxed);
            let stats = sum_tile_stats(&tiles);
            eprintln!(
                "tile-pong-server: {count} pongs queued ({rate:.0} packets/s), lane drops {lane_drops}, tile drops {}",
                stats.classifier_drops + stats.rx_queue_drops + stats.tx_drops
            );
            last_report = now;
            last_reflected = count;
        }
    }

    stop.store(true, Ordering::Relaxed);
    join_lanes(lanes)?;

    let count = reflected.load(Ordering::Relaxed);
    let elapsed = started.elapsed();
    let rate = if elapsed.is_zero() {
        0.0
    } else {
        count as f64 / elapsed.as_secs_f64()
    };
    println!("tile-pong-server: {count} pongs queued in {elapsed:?} ({rate:.0} packets/s)");

    let lane_drops = dropped.load(Ordering::Relaxed);
    if lane_drops > 0 {
        eprintln!("tile-pong-server: dropped {lane_drops} pongs to full lane TX queues");
    }
    let stats = sum_tile_stats(&tiles);
    if stats.classifier_drops + stats.rx_queue_drops + stats.tx_drops > 0 {
        eprintln!("tile-pong-server: tile stats: {stats:?}");
    }
    Ok(())
}

fn run_lane(
    lane_index: usize,
    handles: Vec<XdpTileHandle>,
    stop: &AtomicBool,
    reflected: &AtomicU64,
    dropped: &AtomicU64,
) {
    debug_assert!(
        handles
            .iter()
            .all(|handle| handle.lane_index() == lane_index)
    );
    let mut local_reflected = 0u64;
    let mut local_dropped = 0u64;

    while !stop.load(Ordering::Relaxed) && !shutdown_requested() {
        let mut progressed = false;
        for handle in &handles {
            let (queued, queue_drops) = reflect_rx_batches(handle);
            progressed |= queued != 0 || queue_drops != 0;
            local_reflected += queued as u64;
            local_dropped += queue_drops as u64;
        }

        if local_reflected >= 1024 {
            reflected.fetch_add(local_reflected, Ordering::Relaxed);
            local_reflected = 0;
        }
        if local_dropped >= 1024 {
            dropped.fetch_add(local_dropped, Ordering::Relaxed);
            local_dropped = 0;
        }
        if !progressed {
            std::hint::spin_loop();
        }
    }

    if local_reflected > 0 {
        reflected.fetch_add(local_reflected, Ordering::Relaxed);
    }
    if local_dropped > 0 {
        dropped.fetch_add(local_dropped, Ordering::Relaxed);
    }
}

fn reflect_rx_batches(handle: &XdpTileHandle) -> (usize, usize) {
    let mut queued = 0usize;
    let mut dropped = 0usize;

    while let Some(mut rx_batch) = handle.pop_rx_batch() {
        let mut tx_batch = handle.alloc_tx_batch();
        for packet in rx_batch.drain() {
            let destination = packet.meta().source;
            tx_batch.push(packet.into_transmit(destination));
        }
        handle.recycle_rx_batch(rx_batch);

        if tx_batch.is_empty() {
            handle.recycle_tx_batch(tx_batch);
            continue;
        }

        let batch_len = tx_batch.len();
        match handle.push_tx_batch(tx_batch) {
            Ok(()) => queued += batch_len,
            Err(tx_batch) => {
                dropped += tx_batch.len();
                handle.recycle_tx_batch(tx_batch);
            }
        }
    }

    (queued, dropped)
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

fn join_lanes(lanes: Vec<thread::JoinHandle<()>>) -> Result<(), BoxError> {
    for lane in lanes {
        if lane.join().is_err() {
            return Err("tile-pong-server lane thread panicked".into());
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
