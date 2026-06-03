//! Backend-agnostic core APIs for fast packet socket implementations.
//!
//! This layer defines backend-neutral identifiers and error types without
//! depending on any concrete socket implementation.

#![deny(missing_docs)]
#![deny(unsafe_code)]

pub mod batch;
pub mod buffer;
pub mod error;
pub mod route;
pub mod sys;

pub use batch::{RecvBatch, SendError, TxSlot};
pub use buffer::{
    BufferAccessError, BufferCapabilities, BufferLayout, BufferPool, OwnedPacketBuffer,
    PacketBuffer, PacketBufferMut, QueueBufferConfig, ReserveError, ScatterGather, Segment,
    Segments,
};
pub use error::{DeviceError, DeviceErrorKind, Error};
pub use route::{NeighborId, RouteId, TunnelId};
pub use sys::{HugePageSize, IfIndex, NumaNode, QueueAffinity, QueueId, SocketId};
