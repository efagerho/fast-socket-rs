//! Tokio integration for wait-driven `fast-socket-rs` UDP sockets.
//!
//! The actor in this crate is generic over any wait-driven [`UdpSocket`]. It
//! owns the socket in one Tokio task and lends real socket buffers to async
//! application code through small RAII wrappers.

#![deny(missing_docs)]

#[cfg(not(unix))]
compile_error!("fast-socket-async-rs currently requires Unix wake file descriptors");

#[cfg(unix)]
mod unix;

#[cfg(unix)]
pub use unix::{
    ActorClosed, ActorConfig, ActorRxBatch, ActorRxPacket, ActorTxBuffer, ActorTxMeta,
    ActorTxPacket, AsyncUdpActor, AsyncUdpError, AsyncUdpHandle, AsyncUdpRx, spawn_udp_actor,
    spawn_udp_actor_local,
};
