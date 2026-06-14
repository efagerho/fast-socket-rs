// `common.rs` is included by each binary via `#[path = ...] mod common;` so a
// helper used by only one binary appears unused in the others. Silence both
// `dead_code` and the per-binary `unused_imports` warnings from re-exports.
#![allow(dead_code, unused_imports)]

use std::ffi::{CStr, CString};
use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket as StdUdpSocket};
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd};
use std::ptr;
use std::thread::JoinHandle;
use std::time::Duration;

use clap::ValueEnum;
use fast_socket_os_rs::{OsUdpSocket, OsUdpSocketConfig};
use fast_socket_rs::{BufferLayout, QueueAffinity, QueueId};
use fast_socket_xdp_rs::{
    BusyPollXdpUdpSocket, RouteSnapshot, XdpProgramHandle, XdpQueueSlot, XdpRouteMonitor,
    XdpRouteMonitorHandle, XdpUdpSocket, cpu_for_xdp_queue, xdp_queue_slots_for_interface,
};

// Re-export shared helpers so existing examples can keep referring to them via
// `common::*` without each file having to depend on `fast_socket_benchmarks`
// directly.
pub use fast_socket_benchmarks::{
    BoxError, Progress, XdpProgramMap, attach_xdp_programs_for_slots, dynamic_source_port,
    install_shutdown_signal_handlers, payload, pin_current_thread_to_cpu, shutdown_requested,
    write_sequence, xdp_program_for_slot,
};

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

pub struct MonitoredXdpUdpSocket {
    pub socket: BusyPollXdpUdpSocket,
    route_updates: XdpRouteMonitorHandle,
    _route_monitor_thread: JoinHandle<()>,
}

impl MonitoredXdpUdpSocket {
    /// Applies the latest netlink route snapshot to this socket's queue-local
    /// router. Call from the worker loop before packet work, not per packet.
    pub fn apply_route_updates(&mut self) -> usize {
        self.route_updates.apply_updates(self.socket.routes_mut())
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

pub fn attach_xdp_programs(plans: &[QueuePlan]) -> Result<XdpProgramMap, BoxError> {
    let slots: Vec<XdpQueueSlot> = plans.iter().map(|plan| plan.slot.clone()).collect();
    attach_xdp_programs_for_slots(&slots)
}

pub fn open_xdp_udp_socket(
    slot: &XdpQueueSlot,
    local: SocketAddrV4,
    peer: SocketAddrV4,
    program: &XdpProgramHandle,
) -> Result<MonitoredXdpUdpSocket, BoxError> {
    let routes = RouteSnapshot::from_netlink()?;
    let mut route_monitor = XdpRouteMonitor::new();
    let route_updates = route_monitor.register_queue();
    let route_monitor_thread = route_monitor.start_netlink(slot.queue, Duration::from_secs(1));
    let egress = routes
        .egress_v4_for_interface(*peer.ip(), slot.ifindex, slot.queue)
        .ok_or_else(|| format!("no queue-local netlink route/ARP entry for {}", peer.ip()))?;
    let socket = XdpUdpSocket::builder(slot.ifindex, slot.queue, local)
        .mtu(egress.mtu as usize)
        .route_snapshot(routes)
        .bind_udp_port(local.port())
        .attached_program(program.clone())
        .open_busy_poll()?;
    Ok(MonitoredXdpUdpSocket {
        socket,
        route_updates,
        _route_monitor_thread: route_monitor_thread,
    })
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
            ..Default::default()
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

pub fn bind_udp_socket_to_device(socket: &StdUdpSocket, device: &str) -> Result<(), BoxError> {
    set_bind_to_device(socket.as_raw_fd(), device)
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
            (&raw const value).cast(),
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
            (&raw const cpu).cast(),
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
            (&raw const sockaddr).cast(),
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
