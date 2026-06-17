use std::future::Future;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use clap::ValueEnum;
use fast_socket_async_rs::{
    ActorConfig, AsyncUdpActor, AsyncUdpHandle, AsyncUdpRx, spawn_udp_actor_local,
};
use fast_socket_os_rs::{OsUdpSocket, OsUdpSocketBuilder};
use fast_socket_rs::{BufferLayout, UdpSocket as FastUdpSocket, WaitDrivenDriverKind};
use fast_socket_xdp_rs::{
    BusyPollXdpUdpSocket, InterfaceSelector, PortFilter, RouteSnapshot, WaitDrivenXdpUdpSocket,
    XdpFactory, XdpFactoryBuilder,
};

pub use fast_socket_benchmarks::{
    BoxError, dynamic_source_port, install_shutdown_signal_handlers, payload, shutdown_requested,
    write_sequence,
};

pub const DEFAULT_BATCH_SIZE: usize = 64;
pub const DEFAULT_PAYLOAD_CAPACITY: usize = 2048;
pub const DEFAULT_THREADS: usize = 1;

const IDLE_SLEEP: Duration = Duration::from_micros(50);
const PROGRESS_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum Backend {
    Os,
    Xdp,
}

pub fn normalize_batch_size(batch_size: usize) -> Result<usize, BoxError> {
    if batch_size == 0 {
        return Err("--batch-size must be at least 1".into());
    }
    Ok(batch_size)
}

pub fn normalize_payload_len(payload_len: usize) -> Result<usize, BoxError> {
    if payload_len == 0 {
        return Err("payload length must be at least 1".into());
    }
    Ok(payload_len)
}

pub fn normalize_xdp_bind(device: &str, bind: SocketAddrV4) -> Result<SocketAddrV4, BoxError> {
    if bind.ip().is_unspecified() {
        Ok(SocketAddrV4::new(interface_ipv4_addr(device)?, bind.port()))
    } else {
        Ok(bind)
    }
}

pub fn interface_ipv4_addr(device: &str) -> Result<Ipv4Addr, BoxError> {
    use std::ffi::CStr;
    use std::ptr;

    let mut addrs = ptr::null_mut();
    if unsafe { libc::getifaddrs(&mut addrs) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let _guard = IfAddrs(addrs);

    let mut current = addrs;
    while !current.is_null() {
        let ifaddr = unsafe { &*current };
        if !ifaddr.ifa_addr.is_null()
            && unsafe { (*ifaddr.ifa_addr).sa_family as libc::c_int } == libc::AF_INET
        {
            let name = unsafe { CStr::from_ptr(ifaddr.ifa_name) }.to_string_lossy();
            if name == device {
                let sockaddr = unsafe { &*(ifaddr.ifa_addr.cast::<libc::sockaddr_in>()) };
                let addr = Ipv4Addr::from(sockaddr.sin_addr.s_addr.to_ne_bytes());
                if !addr.is_unspecified() {
                    return Ok(addr);
                }
            }
        }
        current = ifaddr.ifa_next;
    }

    Err(format!("no IPv4 address found on device {device}").into())
}

struct IfAddrs(*mut libc::ifaddrs);

impl Drop for IfAddrs {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { libc::freeifaddrs(self.0) };
        }
    }
}

pub fn open_os_udp_socket(
    device: &str,
    bind: SocketAddrV4,
    batch_size: usize,
    payload_capacity: usize,
) -> Result<OsUdpSocket, BoxError> {
    let layout = BufferLayout::for_payload(payload_capacity);
    Ok(OsUdpSocketBuilder::new(bind.into())
        .bind_to_device(device)
        .buffer_layout(layout)
        .max_batch(batch_size)
        .pool_max_buffers((batch_size * 4).max(128))
        .mtu(1472)
        .bind()?)
}

pub fn build_xdp_factory(
    device: &str,
    local: SocketAddrV4,
    threads: usize,
    routes: RouteSnapshot,
) -> Result<XdpFactory, BoxError> {
    if threads == 0 {
        return Err("--threads must be at least 1".into());
    }
    Ok(
        XdpFactoryBuilder::new(InterfaceSelector::Name(device.to_string()))?
            .threads(threads)
            .port_filter(PortFilter::UdpPorts(vec![local.port()]))
            .route_snapshot(routes)
            .build()?,
    )
}

pub fn open_os_actor(
    device: &str,
    bind: SocketAddrV4,
    batch_size: usize,
    payload_capacity: usize,
) -> Result<AsyncUdpActor<OsUdpSocket>, BoxError> {
    let socket = open_os_udp_socket(device, bind, batch_size, payload_capacity)?;
    Ok(spawn_udp_actor_local(
        socket,
        ActorConfig {
            recv_batch_size: batch_size,
            ..ActorConfig::default()
        },
    )?)
}

pub fn open_xdp_wait_driven_actors(
    device: &str,
    local: SocketAddrV4,
    threads: usize,
    batch_size: usize,
) -> Result<Vec<AsyncUdpActor<WaitDrivenXdpUdpSocket>>, BoxError> {
    let factory = build_xdp_factory(device, local, threads, RouteSnapshot::from_netlink()?)?;
    let mut actors = Vec::new();
    for plan in factory.into_worker_plans() {
        let aggregate = plan.open_udp_wait_driven_unpinned(local)?;
        for socket in aggregate.into_members() {
            actors.push(spawn_udp_actor_local(
                socket,
                ActorConfig {
                    recv_batch_size: batch_size,
                    ..ActorConfig::default()
                },
            )?);
        }
    }
    if actors.is_empty() {
        return Err("XDP factory did not produce any wait-driven sockets".into());
    }
    Ok(actors)
}

pub fn run_os_socket_loop<State, Step>(
    name: &'static str,
    device: &str,
    bind: SocketAddrV4,
    batch_size: usize,
    payload_capacity: usize,
    mut state: State,
    mut step: Step,
) -> Result<(), BoxError>
where
    Step: FnMut(&mut OsUdpSocket, &mut State) -> Result<usize, BoxError>,
{
    let mut socket = open_os_udp_socket(device, bind, batch_size, payload_capacity)?;
    eprintln!("{name}: os socket bound to {bind} on {device}");
    let mut progress = Progress::new(name);
    while !shutdown_requested() {
        let count = step(&mut socket, &mut state)?;
        progress.add(count as u64);
        if count == 0 {
            socket.drain_tx_completions()?;
            thread::sleep(IDLE_SLEEP);
        }
    }
    progress.finish();
    Ok(())
}

pub fn run_xdp_busy_poll_loop<State, Init, Step>(
    name: &'static str,
    device: &str,
    local: SocketAddrV4,
    threads: usize,
    init: Init,
    step: Step,
) -> Result<(), BoxError>
where
    State: Send + 'static,
    Init: Fn() -> State + Send + Sync + 'static,
    Step: Fn(&mut BusyPollXdpUdpSocket, &mut State) -> Result<usize, BoxError>
        + Send
        + Sync
        + 'static,
{
    let factory = build_xdp_factory(device, local, threads, RouteSnapshot::from_netlink()?)?;
    let plans = factory.into_worker_plans();
    eprintln!(
        "{name}: {} XDP aggregate worker(s) bound to {local} on {device}",
        plans.len()
    );

    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));
    let init = Arc::new(init);
    let step = Arc::new(step);
    let mut handles = Vec::with_capacity(plans.len());

    for plan in plans {
        let worker_stop = Arc::clone(&stop);
        let worker_total = Arc::clone(&total);
        let worker_init = Arc::clone(&init);
        let worker_step = Arc::clone(&step);
        let cpu = plan.cpu();
        handles.push(thread::spawn(move || -> Result<(), String> {
            let mut aggregate = plan
                .open_udp_busy_poll(local)
                .map_err(|error| error.to_string())?;
            let mut states = (0..aggregate.len())
                .map(|_| worker_init())
                .collect::<Vec<_>>();
            while !worker_stop.load(Ordering::Relaxed) && !shutdown_requested() {
                let mut progressed = 0usize;
                for (socket, state) in aggregate.members_mut().iter_mut().zip(states.iter_mut()) {
                    let count = worker_step(socket, state).map_err(|error| error.to_string())?;
                    progressed += count;
                    worker_total.fetch_add(count as u64, Ordering::Relaxed);
                }
                if progressed == 0 {
                    aggregate
                        .drain_tx_completions()
                        .map_err(|error| error.to_string())?;
                    thread::sleep(IDLE_SLEEP);
                }
            }
            Ok(())
        }));
        eprintln!("{name}: worker pinned/opening on CPU {cpu}");
    }

    let mut progress = Progress::new(name);
    while !shutdown_requested() && !stop.load(Ordering::Relaxed) {
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
            Ok(Err(error)) => return Err(format!("{name} worker failed: {error}").into()),
            Err(_) => return Err(format!("{name} worker thread panicked").into()),
        }
    }
    progress.set(total.load(Ordering::Relaxed));
    progress.finish();
    Ok(())
}

pub async fn run_actor_tasks<S, F, Fut>(
    name: &'static str,
    actors: Vec<AsyncUdpActor<S>>,
    task: F,
) -> Result<(), BoxError>
where
    S: FastUdpSocket + 'static,
    S::Driver: WaitDrivenDriverKind,
    S::RecvMeta: 'static,
    F: Fn(AsyncUdpHandle<S>, AsyncUdpRx<S>, Arc<AtomicBool>, Arc<AtomicU64>) -> Fut
        + Clone
        + 'static,
    Fut: Future<Output = Result<(), BoxError>> + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::with_capacity(actors.len());
    let mut actor_joins = Vec::with_capacity(actors.len());
    let mut tasks = Vec::with_capacity(actors.len());

    for actor in actors {
        let (handle, rx, join) = actor.into_parts();
        handles.push(handle.clone());
        actor_joins.push(join);
        tasks.push(tokio::task::spawn_local(task.clone()(
            handle,
            rx,
            Arc::clone(&stop),
            Arc::clone(&total),
        )));
    }

    let mut progress = AsyncProgress::new(name);
    while !shutdown_requested() && !stop.load(Ordering::Relaxed) {
        if actor_joins.iter().any(tokio::task::JoinHandle::is_finished) {
            break;
        }
        progress.set(total.load(Ordering::Relaxed));
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    stop.store(true, Ordering::Relaxed);
    for handle in &handles {
        let _ = handle.shutdown().await;
    }
    drop(handles);

    for task in tasks {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error),
            Err(error) => return Err(format!("{name} task failed: {error}").into()),
        }
    }

    for join in actor_joins {
        match join.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(format!("{name} actor failed: {error}").into()),
            Err(error) => return Err(format!("{name} actor task failed: {error}").into()),
        }
    }

    progress.set(total.load(Ordering::Relaxed));
    progress.finish();
    Ok(())
}

pub struct Progress {
    name: &'static str,
    started: Instant,
    last_report: Instant,
    last_count: u64,
    count: u64,
}

impl Progress {
    pub fn new(name: &'static str) -> Self {
        let now = Instant::now();
        Self {
            name,
            started: now,
            last_report: now,
            last_count: 0,
            count: 0,
        }
    }

    pub fn add(&mut self, count: u64) {
        self.set(self.count + count);
    }

    pub fn set(&mut self, count: u64) {
        self.count = count;
        let now = Instant::now();
        if now.duration_since(self.last_report) >= PROGRESS_INTERVAL {
            let interval = now.duration_since(self.last_report).as_secs_f64();
            let rate = (self.count - self.last_count) as f64 / interval;
            eprintln!(
                "{}: {} packets ({rate:.0} packets/s)",
                self.name, self.count
            );
            self.last_report = now;
            self.last_count = self.count;
        }
    }

    pub fn finish(&self) {
        let elapsed = self.started.elapsed();
        let rate = if elapsed.is_zero() {
            0.0
        } else {
            self.count as f64 / elapsed.as_secs_f64()
        };
        println!(
            "{}: {} packets in {:?} ({rate:.0} packets/s)",
            self.name, self.count, elapsed
        );
    }
}

pub struct AsyncProgress(Progress);

impl AsyncProgress {
    pub fn new(name: &'static str) -> Self {
        Self(Progress::new(name))
    }

    pub fn set(&mut self, count: u64) {
        self.0.set(count);
    }

    pub fn finish(&self) {
        self.0.finish();
    }
}
