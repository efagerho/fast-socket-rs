#[path = "../common.rs"]
mod common;

use std::net::{
    IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6, UdpSocket as StdUdpSocket,
};
use std::time::{Duration, Instant};

use clap::Parser;
use common::{
    BoxError, Mode, bind_udp_socket_to_device, dynamic_source_port, install_shutdown_signal_handlers,
    interface_ipv4_addr, payload, shutdown_requested, write_sequence,
};
use fast_socket_os_rs::{OsUdpSocket, OsUdpSocketConfig};
use fast_socket_rs::{
    BufferLayout, PacketBufferMut, QueueAffinity, QueueId, TxSlot, UdpSocket as FastUdpSocket,
    UdpTransmit, UdpTxBuffer, UdpTxBufferMut,
};
use fast_socket_xdp_rs::{
    BusyPollXdpUdpSocket, RouteSnapshot, XdpUdpSocket, if_name_to_index, resolve_xdp_queue_slot,
};

const PAYLOAD_LEN: usize = 64;
const BATCH_SIZE: usize = 64;
const PROGRESS_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Parser)]
struct Args {
    /// Device name to attach or bind to.
    #[arg(long)]
    device: String,

    /// Target UDP endpoint as IP:PORT.
    #[arg(long)]
    target: SocketAddr,

    /// Socket backend to use.
    #[arg(long, value_enum, ignore_case = true)]
    mode: Mode,
}

fn main() -> Result<(), BoxError> {
    install_shutdown_signal_handlers()?;
    let args = Args::parse();

    match args.mode {
        Mode::Xdp => {
            let target = socket_addr_v4(args.target)?;
            let mut socket = open_xdp_socket(&args.device, target)?;
            blaster(&mut socket, target.into())
        }
        Mode::Os => {
            let mut socket = open_os_socket(&args.device, args.target)?;
            blaster(&mut socket, args.target)
        }
    }
}

fn blaster<S>(socket: &mut S, target: SocketAddr) -> Result<(), BoxError>
where
    S: FastUdpSocket,
{
    let started = Instant::now();
    let mut last_report = started;
    let mut last_count = 0u64;
    // Two counters:
    //   `next_sequence` is the next sequence number to stamp into payload
    //   bytes and advances by `batch.len()` whether or not the kernel
    //   accepted the slot — that way sequence numbers stay monotonic on the
    //   wire and a partial accept simply leaves gaps in the sequence space
    //   instead of replaying the same numbers.
    //   `count` is the count of slots the kernel actually accepted; that is
    //   what we report.
    let mut next_sequence = 0u64;
    let mut count = 0u64;
    let mut dropped = 0u64;
    let mut payload_bytes = payload(PAYLOAD_LEN);
    let mut tx_buffers: Vec<UdpTxBufferMut<S>> = Vec::with_capacity(BATCH_SIZE);
    let mut batch: Vec<TxSlot<UdpTransmit<UdpTxBuffer<S>>>> = Vec::with_capacity(BATCH_SIZE);

    while !shutdown_requested() {
        tx_buffers.clear();
        batch.clear();
        socket.allocate_tx_batch(&mut tx_buffers, BATCH_SIZE)?;

        while let Some(mut packet) = tx_buffers.pop() {
            write_sequence(&mut payload_bytes, next_sequence + batch.len() as u64);
            packet.extend_from_slice(&payload_bytes)?;
            batch.push(TxSlot::Ready(UdpTransmit::new(packet.freeze(), target)));
        }

        if batch.is_empty() {
            socket.drain_tx_completions()?;
            std::hint::spin_loop();
            continue;
        }

        let batch_len = batch.len() as u64;
        let accepted = socket.send(batch.as_mut_slice())?;
        if accepted < batch.len() {
            socket.drain_tx_completions()?;
        }
        next_sequence += batch_len;
        count += accepted as u64;
        dropped += batch_len - accepted as u64;

        let now = Instant::now();
        if now.duration_since(last_report) >= PROGRESS_INTERVAL {
            let interval = now.duration_since(last_report).as_secs_f64();
            let rate = (count - last_count) as f64 / interval;
            eprintln!("blast: {count} packets ({rate:.0} packets/s)");
            last_report = now;
            last_count = count;
        }
    }

    let elapsed = started.elapsed();
    let rate = if elapsed.is_zero() {
        0.0
    } else {
        count as f64 / elapsed.as_secs_f64()
    };
    println!("blast: {count} packets in {elapsed:?} ({rate:.0} packets/s)");
    if dropped > 0 {
        eprintln!("blast: dropped {dropped} packets to TX back-pressure (sequence-number gaps)");
    }
    Ok(())
}

fn open_xdp_socket(device: &str, target: SocketAddrV4) -> Result<BusyPollXdpUdpSocket, BoxError> {
    let slot = resolve_xdp_queue_slot(device, QueueId::new(0))?;
    // Ask the kernel for an unused ephemeral port instead of deriving one
    // from the PID: PID-mod-port-range can collide with other concurrent
    // blasters on the same host. There is a brief race window between
    // closing the probe socket and binding it via AF_XDP, but it is
    // dramatically less likely to collide than PID-mod allocation.
    let local_ip = interface_ipv4_addr(device)?;
    let local = SocketAddrV4::new(local_ip, kernel_assigned_udp_port(local_ip)?);
    let _ = dynamic_source_port; // retain helper for callers that still want the legacy behavior
    let routes = RouteSnapshot::from_netlink()?;
    let egress = routes
        .egress_v4_for_interface(*target.ip(), slot.ifindex, slot.queue)
        .ok_or_else(|| format!("no queue-local netlink route/ARP entry for {}", target.ip()))?;
    Ok(XdpUdpSocket::builder(slot.ifindex, slot.queue, local)
        .mtu(egress.mtu as usize)
        .route_snapshot(routes)
        .bind_udp_port(local.port())
        .open_busy_poll()?)
}

fn kernel_assigned_udp_port(local_ip: Ipv4Addr) -> Result<u16, BoxError> {
    let probe = StdUdpSocket::bind(SocketAddrV4::new(local_ip, 0))?;
    let port = probe.local_addr()?.port();
    // `probe` drops here, releasing the port. The next bind on this port
    // may race with another process, but the kernel keeps the port in
    // TIME_WAIT briefly enough that example workloads almost always win
    // the race.
    drop(probe);
    Ok(port)
}

fn open_os_socket(device: &str, target: SocketAddr) -> Result<OsUdpSocket, BoxError> {
    let if_index = if_name_to_index(device)?;
    let socket = StdUdpSocket::bind(unspecified_addr(target))?;
    bind_udp_socket_to_device(&socket, device)?;

    let layout = BufferLayout::with_headroom_and_tailroom(PAYLOAD_LEN.max(2048), 0, 0);
    Ok(OsUdpSocket::from_std(
        socket,
        OsUdpSocketConfig {
            if_index: Some(if_index),
            queue_id: QueueId::new(0),
            queue_affinity: QueueAffinity::Any,
            rx_buffer_layout: layout,
            tx_buffer_layout: layout,
            mtu: udp_payload_mtu(target),
            ..Default::default()
        },
    )?)
}

fn socket_addr_v4(addr: SocketAddr) -> Result<SocketAddrV4, BoxError> {
    match addr {
        SocketAddr::V4(addr) => Ok(addr),
        SocketAddr::V6(_) => Err("XDP mode requires an IPv4 target".into()),
    }
}

fn unspecified_addr(target: SocketAddr) -> SocketAddr {
    match target {
        SocketAddr::V4(_) => SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0).into(),
        SocketAddr::V6(_) => SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0).into(),
    }
}

fn udp_payload_mtu(target: SocketAddr) -> usize {
    match target.ip() {
        IpAddr::V4(_) => 1472,
        IpAddr::V6(_) => 1452,
    }
}
