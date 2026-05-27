#![allow(dead_code)]

use std::collections::BTreeMap;
use std::ffi::{CStr, CString};
use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket as StdUdpSocket};
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd};
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use clap::ValueEnum;
use fast_socket_os_rs::{OsUdpSocket, OsUdpSocketConfig};
use fast_socket_rs::{BufferLayout, IfIndex, QueueAffinity, QueueId};
use fast_socket_xdp_rs::{
    AttachMode, BusyPollXdpUdpSocket, RouteSnapshot, XdpIpPacketSocketBuilder, XdpProgramHandle,
    XdpQueueSlot, XdpUdpSocket, cpu_for_xdp_queue, xdp_queue_slots_for_interface,
};

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type XdpProgramMap = BTreeMap<IfIndex, XdpProgramHandle>;

const FIRST_DYNAMIC_PORT: u16 = 49152;

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum Mode {
    Xdp,
    Os,
}

#[derive(Clone, Debug)]
pub struct QueuePlan {
    pub slot: XdpQueueSlot,
    pub cpu: u32,
}

#[derive(Debug)]
pub struct Progress {
    name: &'static str,
    started: Instant,
    last: Instant,
    last_count: u64,
}

impl Progress {
    pub fn new(name: &'static str) -> Self {
        let now = Instant::now();
        Self {
            name,
            started: now,
            last: now,
            last_count: 0,
        }
    }

    pub fn tick(&mut self, count: u64) {
        if self.last.elapsed() < Duration::from_secs(1) {
            return;
        }
        let elapsed = self.last.elapsed().as_secs_f64();
        let rate = (count - self.last_count) as f64 / elapsed;
        eprintln!("{}: {count} packets ({rate:.0} packets/s)", self.name);
        self.last = Instant::now();
        self.last_count = count;
    }

    pub fn finish(&self, count: u64) {
        let elapsed = self.started.elapsed();
        let rate = if elapsed.is_zero() {
            0.0
        } else {
            count as f64 / elapsed.as_secs_f64()
        };
        println!(
            "{}: {count} packets in {elapsed:?} ({rate:.0} packets/s)",
            self.name
        );
    }
}

pub fn queue_plan(device: &str) -> Result<Vec<QueuePlan>, BoxError> {
    xdp_queue_slots_for_interface(device)?
        .into_iter()
        .map(|slot| {
            let cpu = cpu_for_xdp_queue(&slot)?;
            Ok(QueuePlan { slot, cpu })
        })
        .collect()
}

pub fn attach_xdp_programs(slots: &[QueuePlan]) -> Result<XdpProgramMap, BoxError> {
    let mut programs = XdpProgramMap::new();
    for plan in slots {
        if programs.contains_key(&plan.slot.ifindex) {
            continue;
        }
        let program = XdpProgramHandle::load(plan.slot.ifindex.get(), AttachMode::Default, None)
            .map_err(|error| {
                format!(
                    "attach XDP program to {} (if_index {}): {error}",
                    plan.slot.iface,
                    plan.slot.ifindex.get()
                )
            })?;
        programs.insert(plan.slot.ifindex, program);
    }
    Ok(programs)
}

pub fn xdp_program_for_slot<'a>(
    programs: &'a XdpProgramMap,
    slot: &XdpQueueSlot,
) -> Result<&'a XdpProgramHandle, BoxError> {
    programs.get(&slot.ifindex).ok_or_else(|| {
        format!(
            "no pre-attached XDP program for {} (if_index {})",
            slot.iface,
            slot.ifindex.get()
        )
        .into()
    })
}

pub fn open_xdp_udp_socket(
    slot: &XdpQueueSlot,
    local: SocketAddrV4,
    peer: SocketAddrV4,
    program: &XdpProgramHandle,
) -> Result<BusyPollXdpUdpSocket, BoxError> {
    let routes = RouteSnapshot::from_netlink(slot.queue)?;
    let egress = routes
        .egress_v4_for_interface(*peer.ip(), slot.ifindex, slot.queue)
        .ok_or_else(|| format!("no queue-local netlink route/ARP entry for {}", peer.ip()))?;
    let ip_socket = XdpIpPacketSocketBuilder::new(slot.ifindex, slot.queue)
        .mtu(egress.mtu as usize)
        .route_snapshot(routes)
        .bind_udp_port(local.port())
        .attached_program(program.clone())
        .open_busy_poll_live()?;
    Ok(XdpUdpSocket::new(ip_socket, local))
}

pub fn open_os_udp_socket(
    device: &str,
    bind: SocketAddrV4,
    cpu: u32,
    queue_id: QueueId,
    payload_len: usize,
) -> Result<OsUdpSocket, BoxError> {
    let socket = bind_reuseport_socket_to_device(device, bind, cpu)?;
    let layout = BufferLayout::with_headroom_and_tailroom(payload_len.max(2048), 0, 0);
    Ok(OsUdpSocket::from_std(
        socket,
        OsUdpSocketConfig {
            if_index: None,
            queue_id,
            queue_affinity: QueueAffinity::Core(cpu),
            rx_buffer_layout: layout,
            tx_buffer_layout: layout,
            mtu: 1472,
        },
    )?)
}

pub fn interface_ipv4_addr(device: &str) -> Result<Ipv4Addr, BoxError> {
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

pub fn dynamic_source_port() -> u16 {
    let range = u32::from(u16::MAX) - u32::from(FIRST_DYNAMIC_PORT) + 1;
    FIRST_DYNAMIC_PORT + (std::process::id() % range) as u16
}

pub fn pin_current_thread_to_cpu(cpu: u32) -> Result<(), BoxError> {
    let cpu = usize::try_from(cpu)?;
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
        let result = libc::sched_setaffinity(
            0,
            std::mem::size_of::<libc::cpu_set_t>(),
            &set as *const libc::cpu_set_t,
        );
        if result == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error().into())
        }
    }
}

pub fn install_shutdown_signal_handlers() -> Result<(), BoxError> {
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

pub fn shutdown_requested() -> bool {
    SHUTDOWN_SIGNALS.load(Ordering::Relaxed) > 0
}

fn bind_reuseport_socket_to_device(
    device: &str,
    bind: SocketAddrV4,
    cpu: u32,
) -> Result<StdUdpSocket, BoxError> {
    let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };

    set_bool_socket_option(fd.as_raw_fd(), libc::SOL_SOCKET, libc::SO_REUSEPORT, true)?;
    set_bind_to_device(fd.as_raw_fd(), device)?;
    set_incoming_cpu(fd.as_raw_fd(), cpu)?;
    bind_socket_addr(fd.as_raw_fd(), bind)?;

    Ok(unsafe { StdUdpSocket::from_raw_fd(fd.into_raw_fd()) })
}

fn set_bool_socket_option(
    fd: libc::c_int,
    level: libc::c_int,
    name: libc::c_int,
    value: bool,
) -> Result<(), BoxError> {
    let value: libc::c_int = i32::from(value);
    let result = unsafe {
        libc::setsockopt(
            fd,
            level,
            name,
            (&value as *const libc::c_int).cast(),
            std::mem::size_of_val(&value) as libc::socklen_t,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().into())
    }
}

fn set_bind_to_device(fd: libc::c_int, device: &str) -> Result<(), BoxError> {
    let device = CString::new(device)?;
    let name = device.as_bytes_with_nul();
    let result = unsafe {
        libc::setsockopt(
            fd,
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

fn set_incoming_cpu(fd: libc::c_int, cpu: u32) -> Result<(), BoxError> {
    let cpu: libc::c_int = cpu.try_into()?;
    let result = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_INCOMING_CPU,
            (&cpu as *const libc::c_int).cast(),
            std::mem::size_of_val(&cpu) as libc::socklen_t,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().into())
    }
}

fn bind_socket_addr(fd: libc::c_int, bind: SocketAddrV4) -> Result<(), BoxError> {
    let sockaddr = libc::sockaddr_in {
        sin_family: libc::AF_INET as libc::sa_family_t,
        sin_port: bind.port().to_be(),
        sin_addr: libc::in_addr {
            s_addr: u32::from_ne_bytes(bind.ip().octets()),
        },
        sin_zero: [0; 8],
    };
    let result = unsafe {
        libc::bind(
            fd,
            (&sockaddr as *const libc::sockaddr_in).cast(),
            std::mem::size_of_val(&sockaddr) as libc::socklen_t,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error().into())
    }
}

struct IfAddrs(*mut libc::ifaddrs);

impl Drop for IfAddrs {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { libc::freeifaddrs(self.0) };
        }
    }
}

static SHUTDOWN_SIGNALS: AtomicUsize = AtomicUsize::new(0);

extern "C" fn handle_shutdown_signal(signal: libc::c_int) {
    if SHUTDOWN_SIGNALS.fetch_add(1, Ordering::SeqCst) > 0 {
        unsafe { libc::_exit(128 + signal) };
    }
}
