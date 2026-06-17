use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::Instant;

use clap::{Parser, ValueEnum};
use fast_socket_benchmarks::{BoxError, Progress, RunLimit, payload, write_sequence};
use fast_socket_os_rs::{OsPacketBuf, OsUdpSocket, OsUdpSocketBuilder};
use fast_socket_rs::{BufferLayout, PacketBufferMut, TxSlot, UdpSocket, UdpTransmit};

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Mode {
    Blast,
}

#[derive(Debug, Parser)]
#[command(
    about = "OS UDP sender: blast packets at a destination as fast as the socket accepts them"
)]
struct Cli {
    /// Sender mode (only `blast` is supported).
    #[arg(value_enum)]
    mode: Mode,

    /// Destination endpoint.
    #[arg(long)]
    dest: SocketAddr,

    /// Local bind endpoint.
    #[arg(long, default_value_t = SocketAddr::from(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)))]
    bind: SocketAddr,

    /// Per-packet payload bytes.
    #[arg(long, default_value_t = 64)]
    payload_len: usize,

    /// Number of sender threads. Each opens its own ephemeral socket.
    #[arg(long, default_value_t = 1)]
    threads: usize,

    #[command(flatten)]
    limit: RunLimit,
}

fn main() -> Result<(), BoxError> {
    let cli = Cli::parse();
    let Mode::Blast = cli.mode;

    if cli.threads == 0 {
        return Err("--threads must be at least 1".into());
    }
    if cli.threads > 1 && cli.bind.port() != 0 {
        return Err(
            "--threads greater than 1 requires an ephemeral bind port (use --bind IP:0 or omit --bind)"
                .into(),
        );
    }

    if cli.threads > 1 {
        return run_threaded(cli.threads, cli.bind, cli.dest, cli.payload_len, cli.limit);
    }

    let mut socket = open_socket(cli.bind, cli.payload_len)?;
    blast(&mut socket, cli.dest, cli.payload_len, cli.limit)
}

fn open_socket(bind: SocketAddr, payload_len: usize) -> Result<OsUdpSocket, BoxError> {
    let layout = BufferLayout::with_headroom_and_tailroom(payload_len.max(2048), 0, 0);
    Ok(OsUdpSocketBuilder::new(bind)
        .buffer_layout(layout)
        .mtu(payload_len.max(1472))
        .bind()?)
}

fn run_threaded(
    threads: usize,
    bind: SocketAddr,
    dest: SocketAddr,
    payload_len: usize,
    limit: RunLimit,
) -> Result<(), BoxError> {
    // Start barrier: each worker opens its socket, signals ready state, then
    // spins on `start` until main has seen every signal. Only then does main
    // capture `started`. Without this the per-thread socket-open time leaks
    // into the elapsed wall clock and biases pps low.
    let start = Arc::new(AtomicBool::new(false));
    let (ready_tx, ready_rx) = mpsc::channel::<()>();
    let mut handles = Vec::with_capacity(threads);
    let mut spawned = 0usize;

    for thread_index in 0..threads {
        let thread_limit = thread_limit(limit, thread_index, threads);
        if thread_limit.count == Some(0) {
            continue;
        }

        let worker_start = Arc::clone(&start);
        let worker_ready_tx = ready_tx.clone();
        handles.push(
            thread::Builder::new()
                .name(format!("os-sender-{thread_index}"))
                .spawn(move || -> Result<u64, BoxError> {
                    let mut socket = open_socket(bind, payload_len)?;
                    worker_ready_tx
                        .send(())
                        .map_err(|_| "sender start barrier closed")?;
                    while !worker_start.load(Ordering::Acquire) {
                        std::hint::spin_loop();
                    }
                    blast_worker(&mut socket, dest, payload_len, thread_limit, None)
                })?,
        );
        spawned += 1;
    }
    drop(ready_tx);

    for _ in 0..spawned {
        ready_rx
            .recv()
            .map_err(|_| "sender thread died before reaching start barrier")?;
    }

    let started = Instant::now();
    start.store(true, Ordering::Release);

    let mut count = 0;
    for handle in handles {
        count += handle.join().map_err(|_| "sender thread panicked")??;
    }

    let elapsed = started.elapsed();
    let rate = if elapsed.is_zero() {
        0.0
    } else {
        count as f64 / elapsed.as_secs_f64()
    };
    println!(
        "os-sender blast: {count} packets across {threads} threads in {elapsed:?} ({rate:.0} packets/s)"
    );
    Ok(())
}

fn thread_limit(limit: RunLimit, index: usize, threads: usize) -> RunLimit {
    let count = limit.count.map(|count| {
        let threads = threads as u64;
        let base = count / threads;
        let remainder = count % threads;
        base + u64::from((index as u64) < remainder)
    });
    RunLimit {
        count,
        duration: limit.duration,
    }
}

fn blast(
    socket: &mut OsUdpSocket,
    dest: SocketAddr,
    payload_len: usize,
    limit: RunLimit,
) -> Result<(), BoxError> {
    let mut progress = Progress::new("os-sender blast");
    let count = blast_worker(socket, dest, payload_len, limit, Some(&mut progress))?;
    progress.finish(count);
    Ok(())
}

fn blast_worker(
    socket: &mut OsUdpSocket,
    dest: SocketAddr,
    payload_len: usize,
    limit: RunLimit,
    mut progress: Option<&mut Progress>,
) -> Result<u64, BoxError> {
    let started = Instant::now();
    let mut count = 0;
    let mut bytes = payload(payload_len);
    let mut batch: Vec<TxSlot<UdpTransmit<OsPacketBuf>>> = Vec::with_capacity(64);
    let mut tx_buffers = Vec::with_capacity(64);
    while limit.keep_running(count, started) {
        batch.clear();
        let mut requested = 0usize;
        while requested < 64 && limit.keep_running(count + requested as u64, started) {
            requested += 1;
        }

        tx_buffers.clear();
        if socket.allocate_tx_batch(&mut tx_buffers, requested)? == 0 {
            std::hint::spin_loop();
            continue;
        }

        for mut packet in tx_buffers.drain(..) {
            write_sequence(&mut bytes, count + batch.len() as u64);
            packet.extend_from_slice(&bytes)?;
            batch.push(TxSlot::Ready(UdpTransmit::new(packet.freeze(), dest)));
        }
        count += socket.send_all(&mut batch)? as u64;
        if let Some(progress) = progress.as_deref_mut() {
            progress.tick(count);
        }
    }
    Ok(count)
}
