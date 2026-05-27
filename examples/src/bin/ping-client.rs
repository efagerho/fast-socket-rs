#[path = "../common.rs"]
mod common;

use std::collections::HashSet;
use std::net::SocketAddrV4;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use clap::Parser;
use fast_socket_rs::{
    BufferPool, PacketBuffer, PacketBufferMut, QueueId, RecvBatch, TxSlot, UdpReceive, UdpRecvMeta,
    UdpRxBuffer, UdpSocket as FastUdpSocket, UdpTransmit,
};

use common::{
    attach_xdp_programs, dynamic_source_port, install_shutdown_signal_handlers,
    interface_ipv4_addr, open_os_udp_socket, open_xdp_udp_socket, pin_current_thread_to_cpu,
    queue_plan, shutdown_requested, xdp_program_for_slot, BoxError, Mode, Progress,
};

const PAYLOAD_LEN: usize = 64;
const BATCH_SIZE: usize = 64;
const MAX_OUTSTANDING: usize = 4096;

#[derive(Clone, Copy, Debug)]
struct Ack {
    sequence: u64,
}

#[derive(Debug, Parser)]
struct Args {
    /// Device name whose NIC queues should own the sockets.
    #[arg(long)]
    device: String,

    /// Pong server endpoint as IP:PORT.
    #[arg(long)]
    target: SocketAddrV4,

    /// Socket backend to use.
    #[arg(long, value_enum, ignore_case = true)]
    mode: Mode,
}

fn main() -> Result<(), BoxError> {
    install_shutdown_signal_handlers()?;
    let args = Args::parse();

    match args.mode {
        Mode::Os => run_os(args.device, args.target),
        Mode::Xdp => run_xdp(args.device, args.target),
    }
}

#[allow(clippy::too_many_arguments)]
fn ping_client<S>(
    socket: &mut S,
    worker_id: usize,
    worker_count: usize,
    target: SocketAddrV4,
    ack_txs: &[mpsc::Sender<Ack>],
    ack_rx: &mpsc::Receiver<Ack>,
    stop: &AtomicBool,
    sent: &AtomicU64,
    received: &AtomicU64,
) -> Result<(), BoxError>
where
    S: FastUdpSocket<RecvMeta = UdpRecvMeta>,
{
    let mut sequence = 0u64;
    let mut outstanding = HashSet::with_capacity(MAX_OUTSTANDING);
    let mut payload = [0u8; PAYLOAD_LEN];
    let mut rx: RecvBatch<UdpReceive<UdpRxBuffer<S>, UdpRecvMeta>> =
        RecvBatch::with_capacity(BATCH_SIZE);

    while !stop.load(Relaxed) && !shutdown_requested() {
        drain_acks(ack_rx, &mut outstanding, received);

        if outstanding.len() < MAX_OUTSTANDING {
            write_ping_payload(&mut payload, worker_id, sequence);
            if send_one(socket, target.into(), &payload)? {
                outstanding.insert(sequence);
                sequence = sequence.wrapping_add(1);
                sent.fetch_add(1, Relaxed);
            }
        }

        rx.clear();
        if socket.recv(&mut rx)? > 0 {
            route_pong_acks::<S>(&mut rx, target, worker_count, ack_txs)?;
            socket.drain_tx_completions()?;
        } else {
            socket.drain_tx_completions()?;
            thread::sleep(Duration::from_micros(50));
        }
    }

    Ok(())
}

fn run_os(device: String, target: SocketAddrV4) -> Result<(), BoxError> {
    let plans = queue_plan(&device)?;
    let local = SocketAddrV4::new(interface_ipv4_addr(&device)?, dynamic_source_port());
    eprintln!(
        "ping-client os: {} SO_REUSEPORT sockets bound to {} with SO_INCOMING_CPU",
        plans.len(),
        local
    );
    run_workers(
        "ping-client os",
        plans,
        move |plan, id, count, ack_txs, ack_rx, stop, sent, received| {
            pin_current_thread_to_cpu(plan.cpu)?;
            let mut socket = open_os_udp_socket(
                &device,
                local,
                plan.cpu,
                QueueId::new(plan.slot.flat_index.get()),
                PAYLOAD_LEN,
            )?;
            ping_client(
                &mut socket,
                id,
                count,
                target,
                &ack_txs,
                &ack_rx,
                &stop,
                &sent,
                &received,
            )
        },
    )
}

fn run_xdp(device: String, target: SocketAddrV4) -> Result<(), BoxError> {
    let plans = queue_plan(&device)?;
    let programs = Arc::new(attach_xdp_programs(&plans)?);
    let local = SocketAddrV4::new(interface_ipv4_addr(&device)?, dynamic_source_port());
    eprintln!(
        "ping-client xdp: {} queue sockets bound to {} with egress toward {}",
        plans.len(),
        local,
        target
    );
    run_workers(
        "ping-client xdp",
        plans,
        move |plan, id, count, ack_txs, ack_rx, stop, sent, received| {
            pin_current_thread_to_cpu(plan.cpu)?;
            let program = xdp_program_for_slot(&programs, &plan.slot)?;
            let mut socket = open_xdp_udp_socket(&plan.slot, local, target, program)?;
            ping_client(
                &mut socket,
                id,
                count,
                target,
                &ack_txs,
                &ack_rx,
                &stop,
                &sent,
                &received,
            )
        },
    )
}

#[allow(clippy::type_complexity)]
fn run_workers<F>(name: &'static str, plans: Vec<common::QueuePlan>, run: F) -> Result<(), BoxError>
where
    F: Fn(
            common::QueuePlan,
            usize,
            usize,
            Vec<mpsc::Sender<Ack>>,
            mpsc::Receiver<Ack>,
            Arc<AtomicBool>,
            Arc<AtomicU64>,
            Arc<AtomicU64>,
        ) -> Result<(), BoxError>
        + Send
        + Sync
        + 'static,
{
    let worker_count = plans.len();
    let stop = Arc::new(AtomicBool::new(false));
    let sent = Arc::new(AtomicU64::new(0));
    let received = Arc::new(AtomicU64::new(0));
    let run = Arc::new(run);
    let (error_tx, error_rx) = mpsc::channel::<String>();

    let mut ack_txs = Vec::with_capacity(worker_count);
    let mut ack_rxs = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        let (tx, rx) = mpsc::channel();
        ack_txs.push(tx);
        ack_rxs.push(rx);
    }

    let mut handles = Vec::with_capacity(worker_count);
    for (id, (plan, ack_rx)) in plans.into_iter().zip(ack_rxs).enumerate() {
        let worker_stop = Arc::clone(&stop);
        let worker_sent = Arc::clone(&sent);
        let worker_received = Arc::clone(&received);
        let worker_error_tx = error_tx.clone();
        let worker_run = Arc::clone(&run);
        let worker_ack_txs = ack_txs.clone();
        handles.push(thread::spawn(move || {
            let cpu = plan.cpu;
            if let Err(error) = worker_run(
                plan,
                id,
                worker_count,
                worker_ack_txs,
                ack_rx,
                worker_stop.clone(),
                worker_sent,
                worker_received,
            ) {
                let _ = worker_error_tx.send(format!("worker cpu {cpu}: {error}"));
                worker_stop.store(true, Relaxed);
            }
        }));
    }
    drop(error_tx);

    let mut sent_progress = Progress::new(name);
    while !shutdown_requested() && !stop.load(Relaxed) {
        if let Ok(error) = error_rx.try_recv() {
            stop.store(true, Relaxed);
            join_workers(handles)?;
            return Err(error.into());
        }
        let sent_total = sent.load(Relaxed);
        let received_total = received.load(Relaxed);
        sent_progress.tick(sent_total);
        eprintln!("{}: {} pongs acked", name, received_total);
        thread::sleep(Duration::from_secs(1));
    }

    stop.store(true, Relaxed);
    join_workers(handles)?;
    if let Ok(error) = error_rx.try_recv() {
        return Err(error.into());
    }
    sent_progress.finish(sent.load(Relaxed));
    eprintln!("{}: {} pongs acked", name, received.load(Relaxed));
    Ok(())
}

fn send_one<S>(
    socket: &mut S,
    target: std::net::SocketAddr,
    payload: &[u8],
) -> Result<bool, BoxError>
where
    S: FastUdpSocket,
{
    let Some(mut packet) = socket.tx_pool_mut().allocate() else {
        socket.drain_tx_completions()?;
        return Ok(false);
    };
    packet.extend_from_slice(payload)?;
    let mut batch = [TxSlot::Ready(UdpTransmit::new(packet.freeze(), target))];
    match socket.send(&mut batch) {
        Ok(1) => Ok(true),
        Ok(_) => {
            socket.drain_tx_completions()?;
            Ok(false)
        }
        Err(error) => Err(error.into()),
    }
}

fn route_pong_acks<S>(
    rx: &mut RecvBatch<UdpReceive<UdpRxBuffer<S>, UdpRecvMeta>>,
    target: SocketAddrV4,
    worker_count: usize,
    ack_txs: &[mpsc::Sender<Ack>],
) -> Result<(), BoxError>
where
    S: FastUdpSocket<RecvMeta = UdpRecvMeta>,
{
    for item in rx.drain() {
        if item.meta.source != std::net::SocketAddr::V4(target) {
            continue;
        }
        let Some((owner, sequence)) = read_ping_payload(&item.packet)? else {
            continue;
        };
        if owner >= worker_count {
            continue;
        }
        let _ = ack_txs[owner].send(Ack { sequence });
    }
    Ok(())
}

fn drain_acks(ack_rx: &mpsc::Receiver<Ack>, outstanding: &mut HashSet<u64>, received: &AtomicU64) {
    while let Ok(ack) = ack_rx.try_recv() {
        if outstanding.remove(&ack.sequence) {
            received.fetch_add(1, Relaxed);
        }
    }
}

fn write_ping_payload(payload: &mut [u8; PAYLOAD_LEN], worker_id: usize, sequence: u64) {
    payload.fill(0);
    payload[..4].copy_from_slice(&(worker_id as u32).to_be_bytes());
    payload[4..12].copy_from_slice(&sequence.to_be_bytes());
}

fn read_ping_payload<B>(packet: &B) -> Result<Option<(usize, u64)>, BoxError>
where
    B: PacketBuffer,
{
    if packet.len() < 12 {
        return Ok(None);
    }
    let mut header = [0u8; 12];
    packet.read_at_exact(0, &mut header)?;
    let owner = u32::from_be_bytes(header[..4].try_into().expect("slice length")) as usize;
    let sequence = u64::from_be_bytes(header[4..12].try_into().expect("slice length"));
    Ok(Some((owner, sequence)))
}

fn join_workers(handles: Vec<thread::JoinHandle<()>>) -> Result<(), BoxError> {
    for handle in handles {
        if handle.join().is_err() {
            return Err("ping-client worker thread panicked".into());
        }
    }
    Ok(())
}
