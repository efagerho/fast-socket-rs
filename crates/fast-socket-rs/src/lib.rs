//! Implementation-agnostic core APIs for fast packet socket implementations.
//!
//! This layer defines shared identifiers, errors, buffer traits, batch helpers,
//! polling policies, and UDP/IP packet socket trait surfaces.

#![deny(missing_docs)]
#![deny(unsafe_code)]

pub mod batch;
pub mod buffer;
pub mod error;
pub mod ip_packet;
pub mod policy;
pub mod route;
pub mod sys;
pub mod udp;

pub use batch::{RecvBatch, SendError, TxSlot};
pub use buffer::{
    BufferAccessError, BufferCapabilities, BufferLayout, BufferPool, OwnedPacketBuffer,
    PacketBuffer, PacketBufferMut, QueueBufferConfig, ReserveError, ScatterGather, Segment,
    Segments,
};
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
pub use route::{NeighborId, RouteId, TunnelId};
pub use sys::{HugePageSize, IfIndex, NumaNode, QueueAffinity, QueueId, SocketId};
pub use udp::{
    BusyPollUdpSocket, EcnCodepoint, ReadinessUdpSocket, UdpCapabilities, UdpReceive, UdpRecvMeta,
    UdpRxBuffer, UdpSocket, UdpTransmit, UdpTxBuffer, UdpTxBufferMut,
};
