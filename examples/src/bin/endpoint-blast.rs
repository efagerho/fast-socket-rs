use fast_socket_examples as common;

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use common::{
    BoxError, DEFAULT_BATCH_SIZE, DEFAULT_THREADS, build_xdp_factory, dynamic_source_port,
    install_shutdown_signal_handlers, interface_ipv4_addr, normalize_batch_size,
    normalize_payload_len, payload, shutdown_requested, write_sequence,
};
use fast_socket_rs::{UdpEndpointSpec, UdpSocket as FastUdpSocket};
use fast_socket_xdp_rs::{
    BusyPollXdpUdpSocket, RouteSnapshot, XdpQueueLocalRouter, XdpUdpAggregate,
};

const DEFAULT_DRAIN_EVERY_BATCHES: usize = 2;
const WORKER_HOUSEKEEPING_BATCHES: usize = 1024;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    device: String,
    #[arg(long)]
    target: SocketAddrV4,
    #[arg(long)]
    source_ip: Option<Ipv4Addr>,
    #[arg(long)]
    source_port: Option<u16>,
    #[arg(long, default_value_t = DEFAULT_THREADS)]
    threads: usize,
    #[arg(long, default_value_t = 64)]
    payload_len: usize,
    #[arg(long, default_value_t = DEFAULT_BATCH_SIZE)]
    batch_size: usize,
    #[arg(long, default_value_t = DEFAULT_DRAIN_EVERY_BATCHES)]
    drain_every_batches: usize,
    #[arg(long)]
    duration_ms: Option<u64>,
}

fn main() -> Result<(), BoxError> {
    install_shutdown_signal_handlers()?;
    let args = Args::parse();
    let batch_size = normalize_batch_size(args.batch_size)?;
    let drain_every_batches = normalize_drain_every_batches(args.drain_every_batches)?;
    let payload_len = normalize_payload_len(args.payload_len)?;
    let source_ip = args
        .source_ip
        .map_or_else(|| interface_ipv4_addr(&args.device), Ok)?;
    let source_port = args.source_port.unwrap_or_else(dynamic_source_port);
    let local = SocketAddrV4::new(source_ip, source_port);
    run_endpoint_blast(
        &args.device,
        BlastConfig {
            local,
            target: args.target,
            threads: args.threads,
            payload_len,
            batch_size,
            drain_every_batches,
            duration: args.duration_ms.map(Duration::from_millis),
        },
    )
}

#[derive(Clone, Copy, Debug)]
struct BlastConfig {
    local: SocketAddrV4,
    target: SocketAddrV4,
    threads: usize,
    payload_len: usize,
    batch_size: usize,
    drain_every_batches: usize,
    duration: Option<Duration>,
}

impl BlastConfig {
    fn worker(self) -> WorkerConfig {
        WorkerConfig {
            target: self.target.into(),
            payload_len: self.payload_len,
            batch_size: self.batch_size,
            drain_every_batches: self.drain_every_batches,
            duration: self.duration,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct WorkerConfig {
    target: SocketAddr,
    payload_len: usize,
    batch_size: usize,
    drain_every_batches: usize,
    duration: Option<Duration>,
}

fn normalize_drain_every_batches(value: usize) -> Result<usize, BoxError> {
    if value == 0 {
        return Err("--drain-every-batches must be at least 1".into());
    }
    Ok(value)
}

#[inline]
fn worker_should_stop(stop: &AtomicBool, started: Instant, duration: Option<Duration>) -> bool {
    stop.load(Ordering::Relaxed)
        || shutdown_requested()
        || duration.is_some_and(|duration| started.elapsed() >= duration)
}

#[inline]
fn flush_pending_total(total: &AtomicU64, pending: &mut u64) {
    if *pending == 0 {
        return;
    }
    total.fetch_add(*pending, Ordering::Relaxed);
    *pending = 0;
}

type EndpointAggregate = XdpUdpAggregate<fast_socket_rs::BusyPollDriver, XdpQueueLocalRouter>;
type Endpoint = <BusyPollXdpUdpSocket as FastUdpSocket>::Endpoint;

fn run_endpoint_blast(device: &str, config: BlastConfig) -> Result<(), BoxError> {
    let BlastConfig {
        local,
        payload_len,
        batch_size,
        drain_every_batches,
        duration,
        target,
        threads,
    } = config;
    let routes = RouteSnapshot::from_netlink()?;
    let factory = build_xdp_factory(device, local, threads, routes)?;
    let plans = factory.into_worker_plans();
    eprintln!(
        "endpoint-blast: {} XDP worker(s), {payload_len}-byte payloads, batch size {batch_size}, drain every {drain_every_batches} batch(es), {local} -> {target}",
        plans.len()
    );

    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::with_capacity(plans.len());

    for plan in plans {
        let worker_config = config.worker();
        let worker_stop = Arc::clone(&stop);
        let worker_total = Arc::clone(&total);
        handles.push(thread::spawn(move || -> Result<(), String> {
            let mut aggregate = plan
                .open_udp_busy_poll(local)
                .map_err(|error| error.to_string())?;
            run_worker(&mut aggregate, worker_config, worker_stop, worker_total)
                .map_err(|error| error.to_string())
        }));
    }

    let started = Instant::now();
    let mut progress = common::Progress::new("endpoint-blast");
    while !shutdown_requested() && !stop.load(Ordering::Relaxed) {
        if duration.is_some_and(|duration| started.elapsed() >= duration) {
            break;
        }
        if handles.iter().any(thread::JoinHandle::is_finished) {
            break;
        }
        progress.set(total.load(Ordering::Relaxed));
        thread::sleep(Duration::from_millis(100));
    }

    stop.store(true, Ordering::Relaxed);
    for handle in handles {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => return Err("endpoint-blast worker thread panicked".into()),
        }
    }
    progress.set(total.load(Ordering::Relaxed));
    progress.finish();
    Ok(())
}

fn run_worker(
    aggregate: &mut EndpointAggregate,
    config: WorkerConfig,
    stop: Arc<AtomicBool>,
    total: Arc<AtomicU64>,
) -> Result<(), BoxError> {
    let WorkerConfig {
        target,
        payload_len,
        batch_size,
        drain_every_batches,
        duration,
    } = config;
    let started = Instant::now();
    let payload_bytes = payload(payload_len);
    let mut sequence = 0u64;
    let mut batches_since_drain = 0usize;
    let mut batches_until_housekeeping = WORKER_HOUSEKEEPING_BATCHES;
    let mut pending_total = 0u64;
    let mut endpoints = Vec::with_capacity(aggregate.len());

    for socket in aggregate.members_mut() {
        let mut spec = UdpEndpointSpec::new(target);
        spec.payload_len = Some(payload_len);
        endpoints.push(socket.prepare_udp_endpoint(spec)?);
    }

    if worker_should_stop(&stop, started, duration) {
        return Ok(());
    }

    'running: loop {
        let mut progressed = 0usize;
        for (socket, endpoint) in aggregate.members_mut().iter_mut().zip(endpoints.iter_mut()) {
            let accepted = send_batch(socket, endpoint, &payload_bytes, &mut sequence, batch_size)?;
            progressed += accepted;
            if accepted > 0 {
                pending_total += accepted as u64;
                batches_since_drain += 1;
                if batches_since_drain >= drain_every_batches {
                    socket.drain_tx_completions()?;
                    batches_since_drain = 0;
                }
            }

            batches_until_housekeeping -= 1;
            if batches_until_housekeeping == 0 {
                flush_pending_total(&total, &mut pending_total);
                batches_until_housekeeping = WORKER_HOUSEKEEPING_BATCHES;
                if worker_should_stop(&stop, started, duration) {
                    break 'running;
                }
            }
        }
        if progressed == 0 {
            aggregate.drain_tx_completions()?;
            flush_pending_total(&total, &mut pending_total);
            if worker_should_stop(&stop, started, duration) {
                break;
            }
            thread::yield_now();
        }
    }

    flush_pending_total(&total, &mut pending_total);

    if batches_since_drain > 0 {
        aggregate.drain_tx_completions()?;
    }

    Ok(())
}

fn send_batch(
    socket: &mut BusyPollXdpUdpSocket,
    endpoint: &mut Endpoint,
    payload_bytes: &[u8],
    sequence: &mut u64,
    batch_size: usize,
) -> Result<usize, BoxError> {
    let accepted = socket
        .udp_endpoint_batch(endpoint, batch_size)
        .send(|_, payload| {
            let payload_len = payload_bytes.len();
            let payload = &mut payload[..payload_len];
            payload.copy_from_slice(payload_bytes);
            write_sequence(payload, *sequence);
            *sequence = (*sequence).wrapping_add(1);
            payload_len
        })?;

    if accepted == 0 {
        socket.drain_tx_completions()?;
    }
    Ok(accepted)
}
