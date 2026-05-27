//! AF_XDP-backed IP packet socket implementation crate.
//!
//! This crate provides Linux AF_XDP building blocks and an XDP-shaped
//! [`IpPacketSocket`](fast_socket_rs::IpPacketSocket) implementation. The first
//! implementation exposes the core socket, buffer, egress, and route snapshot
//! surfaces while low-level AF_XDP fd activation is built out behind the same
//! types.

#![deny(missing_docs)]

#[cfg(target_os = "linux")]
pub mod buffer;
#[cfg(target_os = "linux")]
pub mod config;
#[cfg(target_os = "linux")]
pub mod egress;
#[cfg(target_os = "linux")]
pub mod interface;
#[cfg(target_os = "linux")]
pub mod netlink;
#[cfg(target_os = "linux")]
pub mod program;
#[cfg(target_os = "linux")]
pub mod raw_socket;
#[cfg(target_os = "linux")]
pub mod ring;
#[cfg(target_os = "linux")]
pub mod route;
#[cfg(target_os = "linux")]
pub mod route_monitor;
#[cfg(target_os = "linux")]
pub mod socket;
#[cfg(target_os = "linux")]
pub mod umem;

#[cfg(target_os = "linux")]
pub use buffer::{XdpPacketBuf, XdpPacketBufMut, XdpRxPool, XdpTxPool};
#[cfg(target_os = "linux")]
pub use config::{XdpIpPacketSocketBuilder, XdpIpPacketSocketConfig, XdpUdpSocketBuilder};
#[cfg(target_os = "linux")]
pub use egress::{ETHERTYPE_IPV4, ETHERTYPE_IPV6, XdpEgress};
#[cfg(target_os = "linux")]
pub use interface::{
    XdpQueueSlot, bond_slaves, cpu_for_xdp_queue, if_index_to_name, if_name_to_index,
    numa_node_for_interface, resolve_xdp_queue_slot, xdp_queue_slots_for_interface,
};
#[cfg(target_os = "linux")]
pub use program::{
    AttachMode, BOUND_PORT_COUNT_LEN, BOUND_PORTS_LEN, DROP_COUNTERS_LEN, DROP_REASON_UDP_FRAGMENT,
    DROP_REASON_UDP_OPTIONS, MAX_BOUND_PORTS, MAX_QUEUES, XdpProgram, XdpProgramHandle,
    embedded_program_bytes, get_or_load, xdp_program_bytes,
};
#[cfg(target_os = "linux")]
pub use raw_socket::{RawXdpSocket, RingSizes, XdpMode};
#[cfg(target_os = "linux")]
pub use route::{RouteSnapshot, XdpLocalRoutes};
#[cfg(target_os = "linux")]
pub use route_monitor::{XdpRouteMonitor, XdpRouteMonitorHandle};
#[cfg(target_os = "linux")]
pub use socket::{
    BusyPollXdpIpPacketSocket, BusyPollXdpUdpSocket, ReadinessXdpIpPacketSocket,
    ReadinessXdpUdpSocket, XdpIpPacketRecvMeta, XdpIpPacketSocket, XdpQueueLocalRouter,
    XdpRouteContext, XdpUdpRouter, XdpUdpSocket,
};
#[cfg(target_os = "linux")]
pub use umem::{AllocError, PageAlignedMemory, Umem};
