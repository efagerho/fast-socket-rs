//! AF_XDP-backed IP packet socket implementation crate.

#![deny(missing_docs)]

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
