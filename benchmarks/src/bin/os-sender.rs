use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::thread;
use std::time::{Duration, Instant};

use fast_socket_benchmarks::{
    Args, BoxError, Progress, RunLimit, payload, timeout_from_args, write_sequence,
};
use fast_socket_os_rs::{OsPacketBuf, OsPacketBufMut, OsUdpSocket, OsUdpSocketBuilder};
use fast_socket_rs::{
    BufferLayout, BufferPool, PacketBufferMut, RecvBatch, TxSlot, UdpReceive, UdpRecvMeta,
    UdpSocket, UdpTransmit,
};

const USAGE: &str = "usage: os-sender <blast|ping> --dest IP:PORT [--bind IP:PORT] [--payload-len N] [--threads N] [--count N] [--duration-ms N] [--timeout-ms N]";

fn main() -> Result<(), BoxError> {
    let mut args = Args::new();
    let mode = args.mode(USAGE)?;
    let dest: SocketAddr = args.required("--dest")?;
    let bind: SocketAddr =
        args.optional("--bind", SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0).into())?;
    let payload_len = args.optional("--payload-len", 64usize)?;
    let threads = args.optional("--threads", 1usize)?;
    let limit = RunLimit::from_args(&mut args)?;
    let timeout = timeout_from_args(&mut args, 1000)?;
    args.finish()?;

    if threads == 0 {
        return Err("--threads must be at least 1".into());
    }
    if threads > 1 && bind.port() != 0 {
        return Err("--threads greater than 1 requires an ephemeral bind port (use --bind IP:0 or omit --bind)".into());
    }

    if threads > 1 {
        return run_threaded(mode, threads, bind, dest, payload_len, limit, timeout);
    }

    let mut socket = open_socket(bind, payload_len)?;
    match mode.as_str() {
        "blast" => blast(&mut socket, dest, payload_len, limit),
        "ping" => ping(&mut socket, dest, payload_len, limit, timeout),
        _ => Err(format!("unknown mode {mode}\n{USAGE}").into()),
    }
}

fn open_socket(bind: SocketAddr, payload_len: usize) -> Result<OsUdpSocket, BoxError> {
    let layout = BufferLayout::with_headroom_and_tailroom(payload_len.max(2048), 0, 0);
    Ok(OsUdpSocketBuilder::new(bind)
        .buffer_layout(layout)
        .mtu(payload_len.max(1472))
        .bind()?)
}

fn run_threaded(
    mode: String,
    threads: usize,
    bind: SocketAddr,
    dest: SocketAddr,
    payload_len: usize,
    limit: RunLimit,
    timeout: Duration,
) -> Result<(), BoxError> {
    let started = Instant::now();
    let mut handles = Vec::with_capacity(threads);

    for thread_index in 0..threads {
        let thread_limit = thread_limit(limit, thread_index, threads);
        if thread_limit.count == Some(0) {
            continue;
        }

        let mode = mode.clone();
        handles.push(
            thread::Builder::new()
                .name(format!("os-sender-{thread_index}"))
                .spawn(move || -> Result<u64, BoxError> {
                    let mut socket = open_socket(bind, payload_len)?;
                    match mode.as_str() {
                        "blast" => blast_worker(&mut socket, dest, payload_len, thread_limit, None),
                        "ping" => {
                            ping_worker(&mut socket, dest, payload_len, thread_limit, timeout, None)
                        }
                        _ => Err(format!("unknown mode {mode}\n{USAGE}").into()),
                    }
                })?,
        );
    }

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
        "os-sender {mode}: {count} packets across {threads} threads in {elapsed:?} ({rate:.0} packets/s)"
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
    while limit.keep_running(count, started) {
        batch.clear();
        while batch.len() < 64 && limit.keep_running(count + batch.len() as u64, started) {
            write_sequence(&mut bytes, count + batch.len() as u64);
            let mut packet = socket
                .tx_pool_mut()
                .allocate()
                .ok_or("tx allocation failed")?;
            packet.extend_from_slice(&bytes)?;
            batch.push(TxSlot::Ready(UdpTransmit::new(packet.freeze(), dest)));
        }
        count += send_all(socket, &mut batch)? as u64;
        if let Some(progress) = progress.as_deref_mut() {
            progress.tick(count);
        }
    }
    Ok(count)
}

fn send_all(
    socket: &mut OsUdpSocket,
    batch: &mut [TxSlot<UdpTransmit<OsPacketBuf>>],
) -> Result<usize, BoxError> {
    let mut accepted = 0;
    while accepted < batch.len() {
        match socket.send(&mut batch[accepted..]) {
            Ok(0) => std::hint::spin_loop(),
            Ok(n) => accepted += n,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(accepted)
}

fn ping(
    socket: &mut OsUdpSocket,
    dest: SocketAddr,
    payload_len: usize,
    limit: RunLimit,
    timeout: Duration,
) -> Result<(), BoxError> {
    let mut progress = Progress::new("os-sender ping");
    let count = ping_worker(
        socket,
        dest,
        payload_len,
        limit,
        timeout,
        Some(&mut progress),
    )?;
    progress.finish(count);
    Ok(())
}

fn ping_worker(
    socket: &mut OsUdpSocket,
    dest: SocketAddr,
    payload_len: usize,
    limit: RunLimit,
    timeout: Duration,
    mut progress: Option<&mut Progress>,
) -> Result<u64, BoxError> {
    let started = Instant::now();
    let mut count = 0;
    let mut rx: RecvBatch<UdpReceive<OsPacketBufMut, UdpRecvMeta>> = RecvBatch::with_capacity(1);
    let mut bytes = payload(payload_len);
    while limit.keep_running(count, started) {
        write_sequence(&mut bytes, count);
        let mut packet = socket
            .tx_pool_mut()
            .allocate()
            .ok_or("tx allocation failed")?;
        packet.extend_from_slice(&bytes)?;
        let mut batch = [TxSlot::Ready(UdpTransmit::new(packet.freeze(), dest))];
        if socket.send(&mut batch)? != 1 {
            continue;
        }

        rx.clear();
        let wait_started = Instant::now();
        while rx.is_empty() {
            socket.recv(&mut rx)?;
            if wait_started.elapsed() >= timeout {
                return Err("timed out waiting for pong".into());
            }
            std::thread::sleep(Duration::from_micros(50));
        }
        count += 1;
        if let Some(progress) = progress.as_deref_mut() {
            progress.tick(count);
        }
    }
    Ok(count)
}
