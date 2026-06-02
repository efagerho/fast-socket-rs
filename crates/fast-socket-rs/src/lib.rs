//! Backend-agnostic core APIs for fast packet socket implementations.
//!
//! This layer defines backend-neutral identifiers and error types without
//! depending on any concrete socket implementation.

#![deny(missing_docs)]
#![deny(unsafe_code)]

pub mod error;
pub mod route;
pub mod sys;

pub use error::{DeviceError, DeviceErrorKind, Error};
pub use route::{NeighborId, RouteId, TunnelId};
pub use sys::{HugePageSize, IfIndex, NumaNode, QueueAffinity, QueueId, SocketId};
