//! Backend-agnostic core APIs for fast packet socket implementations.
//!
//! This crate owns the shared traits, buffer abstractions, packet metadata, and
//! backend-neutral policy vocabulary. Backend crates depend on this crate; it
//! intentionally has no dependency on OS, AF_XDP, or DPDK crates.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

pub mod batch;
pub mod buffer;
pub mod device;
pub mod error;
pub mod ip_packet;
pub mod policy;
pub mod route;
pub mod sys;
pub mod udp;

pub use batch::{RecvBatch, SendError, TxSlot};
pub use buffer::{
    BufferAccessError, BufferCapabilities, BufferLayout, BufferLayoutError, BufferPool,
    HeapBufferPool, OwnedPacketBuffer, PacketBuf, PacketBufMut, PacketBuffer, PacketBufferMut,
    QueueBufferConfig, ReserveError, ScatterGather, Segment, Segments,
};
pub use device::{Capabilities, RawDevice, RawDeviceStats};
pub use error::{DeviceError, DeviceErrorKind, Error};
pub use ip_packet::{
    BusyPollIpPacketSocket, ChecksumStatus, CoreEgress, IpPacketEgress, IpPacketReceive,
    IpPacketRecvMeta, IpPacketRxBuffer, IpPacketSocket, IpPacketTransmit, IpPacketTxBuffer,
    IpVersion, ReadinessIpPacketSocket, TxOffload,
};
pub use policy::{
    BusyPollDriver, BusyPollDriverMode, IpFamily, Mixed, PollDriver, PollMode, ReadinessDriver,
    ReadinessDriverMode, ReadinessSource, V4Only, V6Only, WaitOutcome, WakeHandle,
};
pub use route::{
    EgressResolver, LinkAddr, NeighborId, NeighborTable, RouteHop, RouteId, RouteTable, TunnelId,
    TunnelTable, TunnelTarget,
};
pub use sys::{HugePageSize, IfIndex, NumaNode, QueueAffinity, QueueId};
pub use udp::{
    BusyPollUdpSocket, EcnCodepoint, ReadinessUdpSocket, UdpCapabilities, UdpReceive, UdpRecvMeta,
    UdpRxBuffer, UdpSocket, UdpTransmit, UdpTxBuffer, UdpTxBufferMut,
};
