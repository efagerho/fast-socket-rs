use std::collections::BTreeSet;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use fast_socket_benchmarks::{
    BoxError, Progress, RunLimit, install_shutdown_signal_handlers, shutdown_requested,
};
use fast_socket_os_rs::{OsPacketBuf, OsPacketBufMut, OsUdpSocket, OsUdpSocketBuilder};
use fast_socket_rs::{
    BufferLayout, IfIndex, PacketBufferMut, QueueAffinity, QueueId, RecvBatch, TxSlot, UdpReceive,
    UdpRecvMeta, UdpSocket, UdpTransmit, pin_current_thread_to_cpu,
};
use fast_socket_xdp_rs::{cpu_for_xdp_queue, if_index_to_name, xdp_queue_slots_for_interface};

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Mode {
    Count,
    Pong,
}

#[derive(Debug, Parser)]
#[command(about = "OS UDP listener: count or reflect received UDP datagrams")]
struct Cli {
    /// Listener mode.
    #[arg(value_enum)]
    mode: Mode,

    /// IPv4 bind endpoint.
    #[arg(long, default_value_t = SocketAddr::from(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 9000)))]
    bind: SocketAddr,

    /// Pin the worker thread to this CPU (mutually exclusive with --incoming-cpu).
    #[arg(long, conflicts_with = "incoming_cpu")]
    cpu: Option<u32>,

    /// Spawn one SO_REUSEPORT worker per RX queue on this interface (name or
    /// numeric ifindex); mutually exclusive with --cpu.
    #[arg(long, conflicts_with = "cpu")]
    incoming_cpu: Option<String>,

    /// Enable SO_REUSEPORT on the bound socket.
    #[arg(long)]
    reuse_port: bool,

    #[command(flatten)]
    limit: RunLimit,
}

fn main() -> Result<(), BoxError> {
    install_shutdown_signal_handlers()?;
    let cli = Cli::parse();

    if let Some(incoming_cpu) = cli.incoming_cpu {
        let cpus = incoming_cpus_from_arg(&incoming_cpu)?;
        return run_incoming_cpu(cli.mode, cli.bind, cpus, cli.limit);
    }

    if let Some(cpu) = cli.cpu {
        pin_current_thread_to_cpu(cpu)?;
    }

    let mut socket = open_socket(cli.bind, cli.reuse_port, cli.cpu, QueueId::new(0))?;
    match cli.mode {
        Mode::Count => count(&mut socket, cli.limit),
        Mode::Pong => pong(&mut socket, cli.limit),
    }
}

fn incoming_cpus_from_arg(value: &str) -> Result<Vec<u32>, BoxError> {
    let iface = match value.parse::<u32>() {
        Ok(ifindex) => if_index_to_name(IfIndex::new(ifindex))?,
        Err(_) => value.to_owned(),
    };

    let mut cpus = BTreeSet::new();
    for slot in xdp_queue_slots_for_interface(&iface)? {
        cpus.insert(cpu_for_xdp_queue(&slot)?);
    }

    if cpus.is_empty() {
        return Err(format!("{iface} has no CPUs handling RX queues").into());
    }

    Ok(cpus.into_iter().collect())
}

fn open_socket(
    bind: SocketAddr,
    reuse_port: bool,
    cpu: Option<u32>,
    queue_id: QueueId,
) -> Result<OsUdpSocket, BoxError> {
    let layout = BufferLayout::with_headroom_and_tailroom(2048, 0, 0);
    let mut builder = OsUdpSocketBuilder::new(bind)
        .buffer_layout(layout)
        .reuse_port(reuse_port)
        .queue_id(queue_id);
    if let Some(cpu) = cpu {
        builder = builder.queue_affinity(QueueAffinity::Core(cpu));
    }
    Ok(builder.bind()?)
}

fn run_incoming_cpu(
    mode: Mode,
    bind: SocketAddr,
    cpus: Vec<u32>,
    limit: RunLimit,
) -> Result<(), BoxError> {
    eprintln!(
        "os-listener: {} SO_REUSEPORT sockets pinned to incoming CPUs {:?}",
        cpus.len(),
        cpus
    );

    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));
    let (error_tx, error_rx) = mpsc::channel::<String>();
    let mut handles = Vec::with_capacity(cpus.len());

    for (index, cpu) in cpus.into_iter().enumerate() {
        let worker_stop = Arc::clone(&stop);
        let worker_total = Arc::clone(&total);
        let worker_error_tx = error_tx.clone();
        handles.push(thread::spawn(move || {
            if let Err(error) = run_incoming_cpu_worker(
                mode,
                bind,
                cpu,
                QueueId::new(index as u32),
                worker_stop.clone(),
                worker_total,
            ) {
                let _ = worker_error_tx.send(format!("worker cpu {cpu}: {error}"));
                worker_stop.store(true, Relaxed);
            }
        }));
    }
    drop(error_tx);

    let started = Instant::now();
    let mut progress = Progress::new(match mode {
        Mode::Count => "os-listener count",
        Mode::Pong => "os-listener pong",
    });

    loop {
        let packets = total.load(Relaxed);
        if !limit.keep_running(packets, started) || shutdown_requested() {
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

fn run_incoming_cpu_worker(
    mode: Mode,
    bind: SocketAddr,
    cpu: u32,
    queue_id: QueueId,
    stop: Arc<AtomicBool>,
    total: Arc<AtomicU64>,
) -> Result<(), BoxError> {
    pin_current_thread_to_cpu(cpu)?;
    let mut socket = open_socket(bind, true, Some(cpu), queue_id)?;

    match mode {
        Mode::Count => count_shared(&mut socket, &stop, &total),
        Mode::Pong => pong_shared(&mut socket, &stop, &total),
    }
}

fn count(socket: &mut OsUdpSocket, limit: RunLimit) -> Result<(), BoxError> {
    let mut progress = Progress::new("os-listener count");
    let started = Instant::now();
    let mut packets = 0;
    let mut rx: RecvBatch<UdpReceive<OsPacketBufMut, UdpRecvMeta>> = RecvBatch::with_capacity(64);
    while limit.keep_running(packets, started) && !shutdown_requested() {
        rx.clear();
        let received = socket.recv(&mut rx)? as u64;
        if received == 0 {
            std::thread::sleep(Duration::from_micros(50));
            continue;
        }
        packets += received;
        progress.tick(packets);
    }
    progress.finish(packets);
    Ok(())
}

fn pong(socket: &mut OsUdpSocket, limit: RunLimit) -> Result<(), BoxError> {
    let mut progress = Progress::new("os-listener pong");
    let started = Instant::now();
    let mut packets = 0;
    let mut rx: RecvBatch<UdpReceive<OsPacketBufMut, UdpRecvMeta>> = RecvBatch::with_capacity(64);
    let mut tx_batch: Vec<TxSlot<UdpTransmit<OsPacketBuf>>> = Vec::with_capacity(64);
    while limit.keep_running(packets, started) && !shutdown_requested() {
        rx.clear();
        if socket.recv(&mut rx)? == 0 {
            std::thread::sleep(Duration::from_micros(50));
            continue;
        }
        tx_batch.clear();
        for item in rx.drain() {
            tx_batch.push(TxSlot::Ready(UdpTransmit::new(
                item.packet.freeze(),
                item.meta.source,
            )));
        }
        packets += socket.send_all(&mut tx_batch)? as u64;
        progress.tick(packets);
    }
    progress.finish(packets);
    Ok(())
}

fn count_shared(
    socket: &mut OsUdpSocket,
    stop: &AtomicBool,
    total: &AtomicU64,
) -> Result<(), BoxError> {
    let mut rx: RecvBatch<UdpReceive<OsPacketBufMut, UdpRecvMeta>> = RecvBatch::with_capacity(64);
    while !stop.load(Relaxed) {
        rx.clear();
        let received = socket.recv(&mut rx)? as u64;
        if received == 0 {
            std::thread::sleep(Duration::from_micros(50));
            continue;
        }
        total.fetch_add(received, Relaxed);
    }
    Ok(())
}

fn pong_shared(
    socket: &mut OsUdpSocket,
    stop: &AtomicBool,
    total: &AtomicU64,
) -> Result<(), BoxError> {
    let mut rx: RecvBatch<UdpReceive<OsPacketBufMut, UdpRecvMeta>> = RecvBatch::with_capacity(64);
    let mut tx_batch: Vec<TxSlot<UdpTransmit<OsPacketBuf>>> = Vec::with_capacity(64);
    while !stop.load(Relaxed) {
        rx.clear();
        if socket.recv(&mut rx)? == 0 {
            std::thread::sleep(Duration::from_micros(50));
            continue;
        }
        tx_batch.clear();
        for item in rx.drain() {
            tx_batch.push(TxSlot::Ready(UdpTransmit::new(
                item.packet.freeze(),
                item.meta.source,
            )));
        }
        total.fetch_add(socket.send_all(&mut tx_batch)? as u64, Relaxed);
    }
    Ok(())
}

fn join_workers(handles: Vec<thread::JoinHandle<()>>) -> Result<(), BoxError> {
    for handle in handles {
        if handle.join().is_err() {
            return Err("os-listener worker thread panicked".into());
        }
    }
    Ok(())
}
