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
use fast_socket_rs::{
    PacketBufferMut, QueueId, TxSlot, UdpSocket as FastUdpSocket, UdpTransmit, UdpTxBuffer,
    UdpTxBufferMut,
};
use fast_socket_xdp_rs::{
    BusyPollXdpUdpSocket, ResolvedL2, RouteSnapshot, XdpEgress, XdpResolvedEgress, XdpRouteContext,
    XdpUdpAggregate, XdpUdpRouter,
};

const DEFAULT_DRAIN_EVERY_BATCHES: usize = 2;

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
    run_static_route_blast(
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

#[derive(Clone, Debug)]
struct StaticTargetRouter {
    target: Ipv4Addr,
    resolved: XdpResolvedEgress,
}

impl XdpUdpRouter for StaticTargetRouter {
    fn resolve_udp_egress(&self, dst: Ipv4Addr, context: XdpRouteContext) -> Option<XdpEgress> {
        if dst != self.target || context.ifindex != self.resolved.egress().ifindex {
            return None;
        }
        let mut egress = self.resolved.egress();
        egress.queue = context.queue;
        Some(egress)
    }

    fn resolve_udp_egress_resolved(
        &self,
        dst: Ipv4Addr,
        context: XdpRouteContext,
    ) -> Option<XdpResolvedEgress> {
        self.resolve_udp_egress(dst, context)
            .map(XdpResolvedEgress::from_egress)
    }

    fn resolve_udp_l2(&self, dst: Ipv4Addr, context: XdpRouteContext) -> Option<ResolvedL2<'_>> {
        if dst != self.target || context.ifindex != self.resolved.egress().ifindex {
            return None;
        }
        Some(ResolvedL2::Borrowed {
            l2_header: self.resolved.l2_header(),
            ip_mtu: context.mtu.min(self.resolved.egress().mtu as usize),
        })
    }
}

type StaticAggregate = XdpUdpAggregate<fast_socket_rs::BusyPollDriver, StaticTargetRouter>;

fn run_static_route_blast(device: &str, config: BlastConfig) -> Result<(), BoxError> {
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
    let factory = build_xdp_factory(device, local, threads, routes.clone())?;
    let plans = factory.into_worker_plans();
    eprintln!(
        "udp-xdp-static-route-blast: {} XDP worker(s), {payload_len}-byte payloads, batch size {batch_size}, drain every {drain_every_batches} batch(es), {local} -> {target}",
        plans.len()
    );

    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::with_capacity(plans.len());

    for plan in plans {
        let queue = plan
            .queue_ids()
            .first()
            .copied()
            .unwrap_or_else(|| QueueId::new(0));
        let egress = routes
            .egress_v4_for_interface(*target.ip(), plan.ifindex(), queue)
            .ok_or_else(|| {
                format!(
                    "no queue-local netlink route/neighbor entry for {} on ifindex {} queue {}",
                    target.ip(),
                    plan.ifindex().get(),
                    queue.get()
                )
            })?;
        let router = StaticTargetRouter {
            target: *target.ip(),
            resolved: XdpResolvedEgress::from_egress(egress),
        };
        let worker_config = config.worker();
        let worker_stop = Arc::clone(&stop);
        let worker_total = Arc::clone(&total);
        handles.push(thread::spawn(move || -> Result<(), String> {
            let mut aggregate = plan
                .open_udp_busy_poll_with_router(local, || router.clone())
                .map_err(|error| error.to_string())?;
            run_worker(&mut aggregate, worker_config, worker_stop, worker_total)
                .map_err(|error| error.to_string())
        }));
    }

    let started = Instant::now();
    let mut progress = common::Progress::new("udp-xdp-static-route-blast");
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
            Err(_) => return Err("udp-xdp-static-route-blast worker thread panicked".into()),
        }
    }
    progress.set(total.load(Ordering::Relaxed));
    progress.finish();
    Ok(())
}

fn run_worker(
    aggregate: &mut StaticAggregate,
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
    let mut payload_bytes = payload(payload_len);
    let mut sequence = 0u64;
    let mut tx_buffers = Vec::with_capacity(batch_size);
    let mut batch = Vec::with_capacity(batch_size);
    let mut batches_since_drain = 0usize;

    while !stop.load(Ordering::Relaxed)
        && !shutdown_requested()
        && duration.is_none_or(|duration| started.elapsed() < duration)
    {
        let mut progressed = 0usize;
        for socket in aggregate.members_mut() {
            let accepted = send_batch(
                socket,
                target,
                &mut payload_bytes,
                &mut sequence,
                batch_size,
                &mut tx_buffers,
                &mut batch,
            )?;
            progressed += accepted;
            if accepted > 0 {
                batches_since_drain += 1;
                if batches_since_drain >= drain_every_batches {
                    socket.drain_tx_completions()?;
                    batches_since_drain = 0;
                }
            }
        }
        if progressed == 0 {
            aggregate.drain_tx_completions()?;
            thread::yield_now();
        } else {
            total.fetch_add(progressed as u64, Ordering::Relaxed);
        }
    }

    if batches_since_drain > 0 {
        aggregate.drain_tx_completions()?;
    }

    Ok(())
}

fn send_batch(
    socket: &mut BusyPollXdpUdpSocket<StaticTargetRouter>,
    target: SocketAddr,
    payload_bytes: &mut [u8],
    sequence: &mut u64,
    batch_size: usize,
    tx_buffers: &mut Vec<UdpTxBufferMut<BusyPollXdpUdpSocket<StaticTargetRouter>>>,
    batch: &mut Vec<TxSlot<UdpTransmit<UdpTxBuffer<BusyPollXdpUdpSocket<StaticTargetRouter>>>>>,
) -> Result<usize, BoxError> {
    tx_buffers.clear();
    batch.clear();
    socket.allocate_tx_batch(tx_buffers, batch_size)?;

    while let Some(mut buffer) = tx_buffers.pop() {
        write_sequence(payload_bytes, *sequence);
        buffer.extend_from_slice(payload_bytes)?;
        batch.push(TxSlot::Ready(UdpTransmit::new(buffer.freeze(), target)));
        *sequence = sequence.wrapping_add(1);
    }

    if batch.is_empty() {
        socket.drain_tx_completions()?;
        return Ok(0);
    }

    let accepted = socket.send(batch.as_mut_slice())?;
    Ok(accepted)
}
