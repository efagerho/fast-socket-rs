#[path = "../common.rs"]
mod common;

use std::net::SocketAddrV4;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use clap::Parser;
use fast_socket_rs::{
    PacketBufferMut, QueueId, RecvBatch, TxSlot, UdpReceive, UdpRecvMeta, UdpRxBuffer,
    UdpSocket as FastUdpSocket, UdpTransmit, UdpTxBuffer,
};

use common::{
    attach_xdp_programs, install_shutdown_signal_handlers, interface_ipv4_addr, open_os_udp_socket,
    open_xdp_udp_socket, pin_current_thread_to_cpu, queue_plan, shutdown_requested,
    xdp_program_for_slot, BoxError, Mode, Progress,
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
}

fn main() -> Result<(), BoxError> {
    install_shutdown_signal_handlers()?;
    let args = Args::parse();

    match args.mode {
        Mode::Os => run_os(args.device, args.target),
        Mode::Xdp => run_xdp(args.device, args.target),
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

    run_workers("pong-server os", plans, move |plan, stop, total| {
        pin_current_thread_to_cpu(plan.cpu)?;
        let mut socket = open_os_udp_socket(
            &device,
            bind,
            plan.cpu,
            QueueId::new(plan.slot.flat_index.get()),
            PAYLOAD_LEN,
        )?;
        pong_server(&mut socket, &stop, &total)
    })
}

fn run_xdp(device: String, target: SocketAddrV4) -> Result<(), BoxError> {
    let plans = queue_plan(&device)?;
    let programs = Arc::new(attach_xdp_programs(&plans)?);
    let local = SocketAddrV4::new(interface_ipv4_addr(&device)?, target.port());
    eprintln!(
        "pong-server xdp: {} queue sockets bound to {} with egress toward {}",
        plans.len(),
        local,
        target.ip()
    );

    run_workers("pong-server xdp", plans, move |plan, stop, total| {
        pin_current_thread_to_cpu(plan.cpu)?;
        let program = xdp_program_for_slot(&programs, &plan.slot)?;
        let mut socket = open_xdp_udp_socket(&plan.slot, local, target, program)?;
        pong_server(&mut socket, &stop, &total)
    })
}

fn run_workers<F>(name: &'static str, plans: Vec<common::QueuePlan>, run: F) -> Result<(), BoxError>
where
    F: Fn(common::QueuePlan, Arc<AtomicBool>, Arc<AtomicU64>) -> Result<(), BoxError>
        + Send
        + Sync
        + 'static,
{
    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));
    let run = Arc::new(run);
    let (error_tx, error_rx) = mpsc::channel::<String>();
    let mut handles = Vec::with_capacity(plans.len());

    for plan in plans {
        let worker_stop = Arc::clone(&stop);
        let worker_total = Arc::clone(&total);
        let worker_error_tx = error_tx.clone();
        let worker_run = Arc::clone(&run);
        handles.push(thread::spawn(move || {
            let cpu = plan.cpu;
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
