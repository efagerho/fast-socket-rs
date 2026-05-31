//! Backend-agnostic core APIs for fast packet socket implementations.
//!
//! This crate owns the shared traits, buffer abstractions, packet metadata, and
//! backend-neutral policy vocabulary. Backend crates depend on this crate; it
//! intentionally has no dependency on OS, AF_XDP, or DPDK crates.

#![deny(missing_docs)]
// `unsafe` is denied crate-wide and permitted only inside the thread-pinning
// helpers in `affinity`, which must issue the `sched_setaffinity` syscall.
#![deny(unsafe_code)]

pub mod affinity;
pub mod batch;
pub mod buffer;
pub mod device;
pub mod error;
pub mod ip_packet;
pub mod policy;
pub mod route;
pub mod sys;
pub mod udp;

pub use affinity::{
    PinOutcome, pin_current_thread_to_affinity, pin_current_thread_to_cpu,
    pin_current_thread_to_ip_packet_socket, pin_current_thread_to_socket,
};
pub use batch::{RecvBatch, SendError, TxSlot};
pub use buffer::{
    BufferAccessError, BufferCapabilities, BufferLayout, BufferPool,
    HeapBufferPool, OwnedPacketBuffer, PacketBuf, PacketBufMut, PacketBuffer, PacketBufferMut,
    QueueBufferConfig, ReserveError, ScatterGather, Segment, Segments,
};
pub use device::{Capabilities, RawDevice, RawDeviceStats};
pub use error::{DeviceError, DeviceErrorKind, Error};
pub use ip_packet::{
    BusyPollIpPacketSocket, ChecksumStatus, CoreEgress, IpPacketEgress, IpPacketReceive,
    IpPacketRecvMeta, IpPacketRxBuffer, IpPacketRxItem, IpPacketSocket, IpPacketTransmit,
    IpPacketTxBuffer, IpPacketTxItem, IpVersion, ReadinessIpPacketSocket, TxOffload,
};
pub use policy::{
    BusyPollDriver, BusyPollDriverMode, IpFamily, Mixed, PollDriver, PollMode, ReadinessDriver,
    ReadinessDriverMode, ReadinessSource, V4Only, V6Only, WaitOutcome, WakeHandle,
};
pub use route::{
    EgressResolver, LinkAddr, LinkAddrParseError, NeighborId, NeighborTable, RouteHop, RouteId,
    RouteTable, TunnelId, TunnelTable, TunnelTarget,
};
pub use sys::{HugePageSize, IfIndex, NumaNode, QueueAffinity, QueueId, SocketId};
pub use udp::{
    BusyPollUdpSocket, EcnCodepoint, ReadinessUdpSocket, UdpCapabilities, UdpReceive, UdpRecvMeta,
    UdpRxBuffer, UdpSocket, UdpTransmit, UdpTxBuffer, UdpTxBufferMut,
};
