use std::ffi::{CStr, CString};
use std::net::{
    IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6, UdpSocket as StdUdpSocket,
};
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use fast_socket_os_rs::{OsUdpSocket, OsUdpSocketConfig};
use fast_socket_rs::{
    BufferLayout, PacketBufferMut, QueueAffinity, QueueId, TxSlot, UdpSocket as FastUdpSocket,
    UdpTransmit, UdpTxBuffer, UdpTxBufferMut,
};
use fast_socket_xdp_rs::{
    if_name_to_index, resolve_xdp_queue_slot, BusyPollXdpUdpSocket, RouteSnapshot,
    XdpIpPacketSocketBuilder, XdpUdpSocket,
};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

const PAYLOAD_LEN: usize = 64;
const BATCH_SIZE: usize = 64;
const PROGRESS_INTERVAL: Duration = Duration::from_secs(1);
const FIRST_DYNAMIC_PORT: u16 = 49152;

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

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Mode {
    Xdp,
    Os,
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
    let mut count = 0u64;
    let mut payload = payload();
    let mut tx_buffers: Vec<UdpTxBufferMut<S>> = Vec::with_capacity(BATCH_SIZE);
    let mut batch: Vec<TxSlot<UdpTransmit<UdpTxBuffer<S>>>> = Vec::with_capacity(BATCH_SIZE);

    while !shutdown_requested() {
        tx_buffers.clear();
        batch.clear();
        socket.allocate_tx_batch(&mut tx_buffers, BATCH_SIZE)?;

        while let Some(mut packet) = tx_buffers.pop() {
            write_sequence(&mut payload, count + batch.len() as u64);
            packet.extend_from_slice(&payload)?;
            batch.push(TxSlot::Ready(UdpTransmit::new(packet.freeze(), target)));
        }

        if batch.is_empty() {
            socket.drain_tx_completions()?;
            std::hint::spin_loop();
            continue;
        }

        let accepted = socket.send(batch.as_mut_slice())?;
        if accepted < batch.len() {
            socket.drain_tx_completions()?;
        }
        count += accepted as u64;

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
    Ok(())
}

fn open_xdp_socket(device: &str, target: SocketAddrV4) -> Result<BusyPollXdpUdpSocket, BoxError> {
    let slot = resolve_xdp_queue_slot(device, QueueId::new(0))?;
    let local = local_addr_for_device(device)?;
    let routes = RouteSnapshot::from_netlink(slot.queue)?;
    let egress = routes
        .egress_v4_for_interface(*target.ip(), slot.ifindex, slot.queue)
        .ok_or_else(|| format!("no queue-local netlink route/ARP entry for {}", target.ip()))?;
    let ip_socket = XdpIpPacketSocketBuilder::new(slot.ifindex, slot.queue)
        .mtu(egress.mtu as usize)
        .bind_udp_port(local.port())
        .open_busy_poll_live()?;
    Ok(XdpUdpSocket::new(ip_socket, local, egress))
}

fn open_os_socket(device: &str, target: SocketAddr) -> Result<OsUdpSocket, BoxError> {
    let if_index = if_name_to_index(device)?;
    let socket = StdUdpSocket::bind(unspecified_addr(target))?;
    bind_to_device(&socket, device)?;

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
        },
    )?)
}

fn local_addr_for_device(device: &str) -> Result<SocketAddrV4, BoxError> {
    Ok(SocketAddrV4::new(
        interface_ipv4_addr(device)?,
        dynamic_source_port(),
    ))
}

fn interface_ipv4_addr(device: &str) -> Result<Ipv4Addr, BoxError> {
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

fn dynamic_source_port() -> u16 {
    let range = u32::from(u16::MAX) - u32::from(FIRST_DYNAMIC_PORT) + 1;
    FIRST_DYNAMIC_PORT + (std::process::id() % range) as u16
}

fn bind_to_device(socket: &StdUdpSocket, device: &str) -> Result<(), BoxError> {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;

        let device = CString::new(device)?;
        let name = device.as_bytes_with_nul();
        let result = unsafe {
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_BINDTODEVICE,
                name.as_ptr().cast(),
                name.len() as libc::socklen_t,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error().into())
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = (socket, device);
        Err("binding OS UDP sockets to a device is only implemented on Linux".into())
    }
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

fn payload() -> [u8; PAYLOAD_LEN] {
    let mut payload = [0u8; PAYLOAD_LEN];
    for (index, byte) in payload.iter_mut().enumerate() {
        *byte = index as u8;
    }
    payload
}

fn write_sequence(payload: &mut [u8], sequence: u64) {
    let bytes = sequence.to_be_bytes();
    let len = payload.len().min(bytes.len());
    payload[..len].copy_from_slice(&bytes[..len]);
}

static SHUTDOWN_SIGNALS: AtomicUsize = AtomicUsize::new(0);

fn install_shutdown_signal_handlers() -> Result<(), BoxError> {
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = handle_shutdown_signal as *const () as usize;
        action.sa_flags = 0;
        libc::sigemptyset(&mut action.sa_mask);
        for signal in [libc::SIGINT, libc::SIGTERM] {
            if libc::sigaction(signal, &action, std::ptr::null_mut()) != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
        }
    }
    Ok(())
}

fn shutdown_requested() -> bool {
    SHUTDOWN_SIGNALS.load(Ordering::Relaxed) > 0
}

extern "C" fn handle_shutdown_signal(signal: libc::c_int) {
    if SHUTDOWN_SIGNALS.fetch_add(1, Ordering::SeqCst) > 0 {
        unsafe { libc::_exit(128 + signal) };
    }
}
