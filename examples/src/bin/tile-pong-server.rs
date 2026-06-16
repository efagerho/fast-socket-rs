#[path = "../common.rs"]
mod common;

use std::net::SocketAddrV4;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use common::{BoxError, install_shutdown_signal_handlers, interface_ipv4_addr, shutdown_requested};
use fast_socket_udp_tile::{SourceAddrClassifier, TileTxPacket, UdpNetworkTileHandle};
use fast_socket_udp_tile_xdp::{XdpUdpTileBuilder, XdpUdpTileHandle, XdpUdpTiles};

const PROGRESS_INTERVAL: Duration = Duration::from_secs(1);

type XdpTileHandle = XdpUdpTileHandle<SourceAddrClassifier>;
type XdpTileSocket = <XdpTileHandle as UdpNetworkTileHandle>::Socket;
type XdpTileSet = XdpUdpTiles<SourceAddrClassifier>;

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
    let tiles = XdpUdpTileBuilder::bind_device(device, local, lane_count)?
        .threads(threads)
        .build()?;

    eprintln!(
        "tile-pong-server xdp: {} tile thread(s), {} lane(s), bound to {} with egress toward {}",
        tiles.len(),
        lane_count,
        local,
        target.ip()
    );

    run_lanes(tiles, lane_count)
}

fn run_lanes(mut tiles: XdpTileSet, lane_count: usize) -> Result<(), BoxError> {
    let stop = Arc::new(AtomicBool::new(false));
    let reflected = Arc::new(AtomicU64::new(0));
    let dropped = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let mut lanes = Vec::with_capacity(lane_count);

    for lane_index in 0..lane_count {
        let handles = tiles
            .lane_handles(lane_index)
            .ok_or_else(|| format!("no tile handles available for lane {lane_index}"))?;
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
        if let Err(error) = tiles.check_worker_threads() {
            stop.store(true, Ordering::Relaxed);
            join_lanes(lanes)?;
            return Err(error.into());
        }

        thread::sleep(Duration::from_millis(200));
        let now = Instant::now();
        if now.duration_since(last_report) >= PROGRESS_INTERVAL {
            let count = reflected.load(Ordering::Relaxed);
            let interval = now.duration_since(last_report).as_secs_f64();
            let rate = (count - last_reflected) as f64 / interval;
            let lane_drops = dropped.load(Ordering::Relaxed);
            let stats = tiles.stats();
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
    let stats = tiles.stats();
    if stats.classifier_drops + stats.rx_queue_drops + stats.tx_drops > 0 {
        eprintln!("tile-pong-server: tile stats: {stats:?}");
    }
    Ok(())
}

fn run_lane(
    lane_index: usize,
    mut handles: Vec<XdpTileHandle>,
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
    let mut tx_packets = Vec::new();

    while !stop.load(Ordering::Relaxed) && !shutdown_requested() {
        let mut progressed = false;
        for handle in &mut handles {
            let (queued, queue_drops) = reflect_rx_batches(handle, &mut tx_packets);
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

fn reflect_rx_batches(
    handle: &mut XdpTileHandle,
    tx_packets: &mut Vec<TileTxPacket<XdpTileSocket>>,
) -> (usize, usize) {
    let mut queued = 0usize;
    let mut dropped = 0usize;

    while let Some(mut rx_batch) = handle.pop_rx_batch() {
        tx_packets.clear();
        for packet in rx_batch.drain() {
            let destination = packet.meta().source;
            let source_port = packet.meta().destination_port;
            let mut transmit = packet.into_transmit(destination);
            transmit.source_port = source_port;
            tx_packets.push(transmit);
        }
        handle.recycle_rx_batch(rx_batch);

        if tx_packets.is_empty() {
            continue;
        }

        queued += handle.push_tx_packets(tx_packets);
        if !tx_packets.is_empty() {
            dropped += tx_packets.len();
            tx_packets.clear();
        }
    }

    (queued, dropped)
}

fn join_lanes(lanes: Vec<thread::JoinHandle<()>>) -> Result<(), BoxError> {
    for lane in lanes {
        if lane.join().is_err() {
            return Err("tile-pong-server lane thread panicked".into());
        }
    }
    Ok(())
}
