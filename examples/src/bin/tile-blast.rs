#[path = "../common.rs"]
mod common;

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket as StdUdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use common::{
    BoxError, install_shutdown_signal_handlers, interface_ipv4_addr, payload, shutdown_requested,
    write_sequence,
};
use fast_socket_udp_tile::{
    AcceptAllClassifier, TileConfig, TileStats, TileTxMeta, UdpNetworkTileHandle,
};
use fast_socket_udp_tile_xdp::{
    ParkXdpUdpTileHandle, ParkXdpUdpTiles, TileWorkerError, XdpUdpTileBuilder, XdpUdpTileHandle,
    XdpUdpTiles,
};

const BATCH_SIZE: usize = 128;
const PAYLOAD_LEN: usize = 64;
const PROGRESS_INTERVAL: Duration = Duration::from_secs(1);
const READY_WAIT_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug, Parser)]
struct Args {
    /// Device name to attach to.
    #[arg(long)]
    device: String,

    /// Target UDP endpoint as IPv4:PORT.
    #[arg(long)]
    target: SocketAddrV4,

    /// Source IPv4 address for the local socket address. Defaults to the IP on --device.
    #[arg(long)]
    source_ip: Option<Ipv4Addr>,

    /// Number of XDP tile threads. All NIC queues are split into this many
    /// contiguous worker plans. Must divide the queue count.
    #[arg(long, default_value_t = 1)]
    threads: usize,

    /// Number of application producer lanes feeding each tile.
    #[arg(long, default_value_t = 1)]
    lane_count: usize,

    /// Tile/socket polling mode: spin uses busy-poll XDP sockets, park uses
    /// wait-driven XDP sockets and sleeps on RX/TX wake fds.
    #[arg(long, value_enum, default_value_t = TilePollModeArg::Spin)]
    poll_mode: TilePollModeArg,

    /// UDP payload length.
    #[arg(long, default_value_t = PAYLOAD_LEN)]
    payload_len: usize,

    /// Stop after this many milliseconds instead of waiting for Ctrl-C.
    #[arg(long)]
    duration_ms: Option<u64>,
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
        args.source_ip,
        args.target,
        args.threads,
        args.lane_count,
        args.poll_mode,
        args.payload_len,
        args.duration_ms.map(Duration::from_millis),
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum TilePollModeArg {
    Spin,
    Park,
}

impl TilePollModeArg {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Spin => "spin",
            Self::Park => "park",
        }
    }
}

fn run_tile_blast(
    device: &str,
    source_ip: Option<Ipv4Addr>,
    target: SocketAddrV4,
    threads: usize,
    lane_count: usize,
    poll_mode: TilePollModeArg,
    payload_len: usize,
    duration: Option<Duration>,
) -> Result<(), BoxError> {
    let source_ip = match source_ip {
        Some(source_ip) => source_ip,
        None => interface_ipv4_addr(device)?,
    };
    let local = SocketAddrV4::new(source_ip, kernel_assigned_udp_port(source_ip)?);
    let config = TileConfig {
        batch_size: BATCH_SIZE,
        ..TileConfig::default()
    };
    match poll_mode {
        TilePollModeArg::Spin => {
            let tiles = XdpUdpTileBuilder::bind_device(device, local, lane_count)?
                .threads(threads)
                .classifier(AcceptAllClassifier)
                .config(config)
                .build()?;
            eprintln!(
                "tile-blast xdp {}: {} tile thread(s), {} producer lane(s), sending {}-byte UDP payloads from {local} to {target}",
                poll_mode.as_str(),
                tiles.len(),
                lane_count,
                payload_len
            );
            run_producers(tiles, target.into(), lane_count, payload_len, duration)
        }
        TilePollModeArg::Park => {
            let tiles = XdpUdpTileBuilder::bind_device(device, local, lane_count)?
                .threads(threads)
                .classifier(AcceptAllClassifier)
                .config(config)
                .build_park()?;
            eprintln!(
                "tile-blast xdp {}: {} tile thread(s), {} producer lane(s), sending {}-byte UDP payloads from {local} to {target}",
                poll_mode.as_str(),
                tiles.len(),
                lane_count,
                payload_len
            );
            run_producers(tiles, target.into(), lane_count, payload_len, duration)
        }
    }
}

trait TileSet {
    type Handle: UdpNetworkTileHandle;

    fn lane_handles(&mut self, lane_index: usize) -> Option<Vec<Self::Handle>>;
    fn stats(&self) -> TileStats;
    fn check_worker_threads(&mut self) -> Result<(), TileWorkerError>;
}

impl TileSet for XdpUdpTiles<AcceptAllClassifier> {
    type Handle = XdpUdpTileHandle<AcceptAllClassifier>;

    fn lane_handles(&mut self, lane_index: usize) -> Option<Vec<Self::Handle>> {
        self.lane_handles(lane_index)
    }

    fn stats(&self) -> TileStats {
        self.stats()
    }

    fn check_worker_threads(&mut self) -> Result<(), TileWorkerError> {
        self.check_worker_threads()
    }
}

impl TileSet for ParkXdpUdpTiles<AcceptAllClassifier> {
    type Handle = ParkXdpUdpTileHandle<AcceptAllClassifier>;

    fn lane_handles(&mut self, lane_index: usize) -> Option<Vec<Self::Handle>> {
        self.lane_handles(lane_index)
    }

    fn stats(&self) -> TileStats {
        self.stats()
    }

    fn check_worker_threads(&mut self) -> Result<(), TileWorkerError> {
        self.check_worker_threads()
    }
}

fn run_producers<T>(
    mut tiles: T,
    target: SocketAddr,
    lane_count: usize,
    payload_len: usize,
    duration: Option<Duration>,
) -> Result<(), BoxError>
where
    T: TileSet,
{
    let stop = Arc::new(AtomicBool::new(false));
    let start = Arc::new(AtomicBool::new(false));
    let queued = Arc::new(AtomicU64::new(0));
    let dropped = Arc::new(AtomicU64::new(0));
    let next_sequence = Arc::new(AtomicU64::new(0));
    let (error_tx, error_rx) = mpsc::channel::<String>();
    let (ready_tx, ready_rx) = mpsc::channel::<usize>();
    let mut producers = Vec::with_capacity(lane_count);

    for lane_index in 0..lane_count {
        let handles = tiles
            .lane_handles(lane_index)
            .ok_or_else(|| format!("no tile handles available for lane {lane_index}"))?;
        let stop = Arc::clone(&stop);
        let start = Arc::clone(&start);
        let queued = Arc::clone(&queued);
        let dropped = Arc::clone(&dropped);
        let next_sequence = Arc::clone(&next_sequence);
        let error_tx = error_tx.clone();
        let ready_tx = ready_tx.clone();
        producers.push(thread::spawn(move || {
            if ready_tx.send(lane_index).is_err() || !wait_for_producer_start(&start, &stop) {
                return;
            }
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
    drop(ready_tx);

    if let Err(error) =
        wait_for_producers_ready(lane_count, &ready_rx, &error_rx, &mut tiles, stop.as_ref())
    {
        stop.store(true, Ordering::Relaxed);
        join_producers(producers)?;
        return Err(error);
    }

    let started = Instant::now();
    let mut last_report = started;
    let mut last_sent = tiles.stats().tx_packets;
    start.store(true, Ordering::Release);

    while !shutdown_requested() && !stop.load(Ordering::Relaxed) {
        if let Ok(error) = error_rx.try_recv() {
            stop.store(true, Ordering::Relaxed);
            join_producers(producers)?;
            return Err(error.into());
        }
        if let Err(error) = tiles.check_worker_threads() {
            stop.store(true, Ordering::Relaxed);
            join_producers(producers)?;
            return Err(error.into());
        }
        if duration.is_some_and(|duration| started.elapsed() >= duration) {
            break;
        }

        thread::sleep(Duration::from_millis(100));
        let now = Instant::now();
        if now.duration_since(last_report) >= PROGRESS_INTERVAL {
            let stats = tiles.stats();
            let sent = stats.tx_packets;
            let queued_count = queued.load(Ordering::Relaxed);
            let interval = now.duration_since(last_report).as_secs_f64();
            let rate = (sent - last_sent) as f64 / interval;
            let lane_drops = dropped.load(Ordering::Relaxed);
            eprintln!(
                "tile-blast: {sent} packets sent ({rate:.0} packets/s), {queued_count} packets queued, lane drops {lane_drops}, tile drops {}",
                stats.classifier_drops + stats.rx_queue_drops + stats.tx_drops
            );
            last_report = now;
            last_sent = sent;
        }
    }

    let elapsed = started.elapsed();
    let final_stats = tiles.stats();
    stop.store(true, Ordering::Relaxed);
    join_producers(producers)?;

    if let Ok(error) = error_rx.try_recv() {
        return Err(error.into());
    }

    let sent = final_stats.tx_packets;
    let queued_count = queued.load(Ordering::Relaxed);
    let rate = if elapsed.is_zero() {
        0.0
    } else {
        sent as f64 / elapsed.as_secs_f64()
    };
    println!(
        "tile-blast: {sent} packets sent in {elapsed:?} ({rate:.0} packets/s), {queued_count} packets queued"
    );

    let lane_drops = dropped.load(Ordering::Relaxed);
    if lane_drops > 0 {
        eprintln!("tile-blast: dropped {lane_drops} packets to full lane TX queues");
    }
    if final_stats.classifier_drops + final_stats.rx_queue_drops + final_stats.tx_drops > 0 {
        eprintln!("tile-blast: tile stats: {final_stats:?}");
    }
    Ok(())
}

fn wait_for_producers_ready(
    lane_count: usize,
    ready_rx: &mpsc::Receiver<usize>,
    error_rx: &mpsc::Receiver<String>,
    tiles: &mut impl TileSet,
    stop: &AtomicBool,
) -> Result<(), BoxError> {
    let mut ready = 0usize;
    while ready < lane_count {
        if let Ok(error) = error_rx.try_recv() {
            return Err(error.into());
        }
        if let Err(error) = tiles.check_worker_threads() {
            return Err(error.into());
        }
        if shutdown_requested() || stop.load(Ordering::Relaxed) {
            return Err("tile-blast stopped before producers were ready".into());
        }
        match ready_rx.recv_timeout(READY_WAIT_INTERVAL) {
            Ok(_) => ready += 1,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err("tile-blast producer setup failed before all lanes were ready".into());
            }
        }
    }
    Ok(())
}

fn wait_for_producer_start(start: &AtomicBool, stop: &AtomicBool) -> bool {
    while !start.load(Ordering::Acquire) {
        if stop.load(Ordering::Relaxed) || shutdown_requested() {
            return false;
        }
        std::hint::spin_loop();
    }
    true
}

fn join_producers(producers: Vec<thread::JoinHandle<()>>) -> Result<(), BoxError> {
    for producer in producers {
        if producer.join().is_err() {
            return Err("tile-blast producer thread panicked".into());
        }
    }
    Ok(())
}

fn run_lane(
    lane_index: usize,
    mut handles: Vec<impl UdpNetworkTileHandle>,
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

            for (offset, buffer) in tx_buffers.iter_mut().enumerate() {
                write_sequence(&mut payload_bytes, base_sequence + offset as u64);
                buffer
                    .extend_from_slice(&payload_bytes)
                    .map_err(|error| error.to_string())?;
            }

            let accepted = handle.push_tx_buffers(&mut tx_buffers, TileTxMeta::new(target));
            local_queued += accepted as u64;
            if !tx_buffers.is_empty() {
                local_dropped += tx_buffers.len() as u64;
                tx_buffers.clear();
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

fn drain_lane_rx(handle: &impl UdpNetworkTileHandle) {
    while let Some(batch) = handle.pop_rx_batch() {
        handle.recycle_rx_batch(batch);
    }
}

fn kernel_assigned_udp_port(local_ip: Ipv4Addr) -> Result<u16, BoxError> {
    let probe = StdUdpSocket::bind(SocketAddrV4::new(local_ip, 0))?;
    let port = probe.local_addr()?.port();
    drop(probe);
    Ok(port)
}
