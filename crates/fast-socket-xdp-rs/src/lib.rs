//! AF_XDP-backed IP packet socket implementation crate.
//!
//! This crate provides Linux AF_XDP building blocks and an XDP-shaped
//! [`IpPacketSocket`](fast_socket_rs::IpPacketSocket) implementation. The first
//! implementation exposes the core socket, buffer, egress, and route snapshot
//! surfaces while low-level AF_XDP fd activation is built out behind the same
//! types.

#![deny(missing_docs)]

#[cfg(target_os = "linux")]
pub mod aggregate;
#[cfg(target_os = "linux")]
pub mod buffer;
#[cfg(target_os = "linux")]
pub mod config;
#[cfg(target_os = "linux")]
pub mod egress;
#[cfg(target_os = "linux")]
pub mod factory;
#[cfg(target_os = "linux")]
pub mod interface;
#[cfg(target_os = "linux")]
pub mod netlink;
#[cfg(target_os = "linux")]
pub mod program;
// Low-level AF_XDP primitives are crate-internal: they expose raw pointers and
// panic-capable accessors and are not part of the stable surface. The high-level
// types are built on them via `crate::` paths; power users can reach the raw
// building blocks through the `unstable-internals`-gated `internals` module.
//
// Parts of each module's API (single-item ring cursors, index-based UMEM
// accessors, the shared-UMEM constructor, …) are used only by tests and by the
// `internals` surface, not by the crate's own data path, so they read as dead
// code when that surface is compiled out. Allow it in exactly that config; with
// the feature on these are reachable public API and the lint is inert.
#[cfg(target_os = "linux")]
#[cfg_attr(not(feature = "unstable-internals"), allow(dead_code))]
pub(crate) mod raw_socket;
#[cfg(target_os = "linux")]
#[cfg_attr(not(feature = "unstable-internals"), allow(dead_code))]
pub(crate) mod ring;
#[cfg(target_os = "linux")]
pub mod route;
#[cfg(target_os = "linux")]
pub mod route_monitor;
#[cfg(target_os = "linux")]
pub mod socket;
#[cfg(target_os = "linux")]
#[cfg_attr(not(feature = "unstable-internals"), allow(dead_code))]
pub(crate) mod umem;

#[cfg(target_os = "linux")]
pub use aggregate::{XdpIpPacketAggregate, XdpUdpAggregate};
#[cfg(target_os = "linux")]
pub use buffer::{XdpPacketBuf, XdpPacketBufMut, XdpRxPool, XdpTxPool};
#[cfg(target_os = "linux")]
pub use config::{XdpIpPacketSocketBuilder, XdpIpPacketSocketConfig, XdpUdpSocketBuilder};
#[cfg(target_os = "linux")]
pub use egress::{ETHERTYPE_IPV4, ETHERTYPE_IPV6, XdpEgress, XdpResolvedEgress};
#[cfg(target_os = "linux")]
pub use factory::{
    CoreAssignmentFn, InterfaceSelector, PortFilter, QueueClaim, XdpFactory, XdpFactoryBuilder,
    XdpWorkerPlan, resolve_interface_index,
};
#[cfg(target_os = "linux")]
pub use interface::{
    XdpQueueSlot, bond_slaves, cpu_for_xdp_queue, if_index_to_name, if_name_to_index,
    numa_node_for_interface, resolve_xdp_queue_slot, xdp_queue_slots_for_interface,
};
#[cfg(target_os = "linux")]
pub use program::{
    AttachMode, BOUND_PORT_COUNT_LEN, BOUND_PORTS_LEN, MAX_BOUND_PORTS, MAX_QUEUES, XdpProgram,
    XdpProgramHandle, embedded_program_bytes, get_or_load, xdp_program_bytes,
};
// `RingSizes`/`XdpMode` are part of the configuration surface (see
// `XdpIpPacketSocketConfig`), so they stay public; the raw socket itself does
// not — it lives behind `internals`.
#[cfg(target_os = "linux")]
pub use raw_socket::{RingSizes, XdpMode};
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
/// Unstable low-level AF_XDP building blocks.
///
/// **Not covered by semver — opt in via the `unstable-internals` feature.**
/// These are the raw socket, UMEM, and descriptor-ring primitives the
/// high-level sockets are built from. They hand out raw pointers and have
/// accessors that panic on misuse (for example `Umem::slice_at`), so they are
/// kept off the default public surface. Reach for them only when building a
/// custom AF_XDP data path, and expect breaking changes between releases.
#[cfg(all(target_os = "linux", feature = "unstable-internals"))]
pub mod internals {
    pub use crate::raw_socket::RawXdpSocket;
    pub use crate::ring::{RingConsumer, RingMmap, RingProducer, RingRange, XdpDesc, mmap_ring};
    pub use crate::umem::{AllocError, PageAlignedMemory, Umem};
}
