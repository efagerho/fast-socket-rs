//! AF_XDP-backed IP packet socket implementation crate.

#![deny(missing_docs)]

#[cfg(target_os = "linux")]
pub mod interface;
#[cfg(target_os = "linux")]
pub mod netlink;
#[cfg(target_os = "linux")]
pub mod program;
#[cfg(target_os = "linux")]
#[cfg_attr(not(feature = "unstable-internals"), allow(dead_code))]
pub(crate) mod raw_socket;
#[cfg(target_os = "linux")]
#[cfg_attr(not(feature = "unstable-internals"), allow(dead_code))]
pub(crate) mod ring;
#[cfg(target_os = "linux")]
#[cfg_attr(not(feature = "unstable-internals"), allow(dead_code))]
pub(crate) mod umem;

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
#[cfg(target_os = "linux")]
pub use raw_socket::{RingSizes, XdpMode};

/// Unstable low-level AF_XDP building blocks.
///
/// **Not covered by semver — opt in via the `unstable-internals` feature.**
#[cfg(all(target_os = "linux", feature = "unstable-internals"))]
pub mod internals {
    /// Raw AF_XDP socket.
    pub use crate::raw_socket::RawXdpSocket;
    /// AF_XDP descriptor-ring helpers.
    pub use crate::ring::{RingConsumer, RingMmap, RingProducer, RingRange, XdpDesc, mmap_ring};
    /// UMEM allocation helpers.
    pub use crate::umem::{AllocError, PageAlignedMemory, Umem};
}
