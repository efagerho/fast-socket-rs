//! UDP-facing socket traits and packet metadata.

use core::num::NonZeroU16;
use std::net::{IpAddr, SocketAddr};

use crate::{
    Error, PacketBufferMut, PollDriver, QueueAffinity, RecvBatch, SendError, SocketId, TxSlot,
};

/// Explicit Congestion Notification codepoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EcnCodepoint {
    /// Not ECN-capable transport.
    NotEct,
    /// ECN-capable transport 0.
    Ect0,
    /// ECN-capable transport 1.
    Ect1,
    /// Congestion encountered.
    Ce,
}

/// Default UDP receive metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UdpRecvMeta {
    /// Remote source address.
    pub source: SocketAddr,
    /// Local destination IP when available.
    pub destination: Option<IpAddr>,
    /// Local destination port when available.
    pub destination_port: Option<u16>,
    /// ECN codepoint when available.
    pub ecn: Option<EcnCodepoint>,
    /// UDP payload length in bytes.
    pub len: usize,
    /// GRO stride when the received buffer contains coalesced UDP datagrams.
    pub gro_stride: Option<NonZeroU16>,
}

/// One UDP packet received by a socket.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UdpReceive<B, M = UdpRecvMeta> {
    /// Packet payload buffer.
    pub packet: B,
    /// Receive metadata.
    pub meta: M,
}

impl<B, M> UdpReceive<B, M> {
    /// Creates a received UDP packet from a buffer and metadata.
    #[must_use]
    pub const fn new(packet: B, meta: M) -> Self {
        Self { packet, meta }
    }
}

/// One UDP packet submitted for transmit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UdpTransmit<B> {
    /// Packet payload buffer.
    pub packet: B,
    /// Remote destination address.
    pub destination: SocketAddr,
    /// Optional source IP selection.
    pub source_ip: Option<IpAddr>,
    /// Optional source UDP port selection.
    pub source_port: Option<u16>,
    /// Optional ECN codepoint.
    pub ecn: Option<EcnCodepoint>,
    /// Optional UDP segmentation size.
    pub gso_segment_size: Option<NonZeroU16>,
}

impl<B> UdpTransmit<B> {
    /// Creates a UDP transmit item for a destination address.
    #[must_use]
    pub const fn new(packet: B, destination: SocketAddr) -> Self {
        Self {
            packet,
            destination,
            source_ip: None,
            source_port: None,
            ecn: None,
            gso_segment_size: None,
        }
    }
}

/// Capability flags for a UDP socket.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct UdpCapabilities {
    /// Transmit path supports UDP segmentation offload.
    pub gso: bool,
    /// Receive path may deliver GRO-coalesced UDP buffers.
    pub gro: bool,
    /// Maximum GSO segment count when known.
    pub max_gso_segments: Option<NonZeroU16>,
}

/// Immutable transmit buffer type derived from a UDP socket's transmit buffer type.
pub type UdpTxBuffer<S> = <<S as UdpSocket>::TxBufferMut as PacketBufferMut>::Frozen;

/// Mutable transmit buffer type allocated by a UDP socket.
pub type UdpTxBufferMut<S> = <S as UdpSocket>::TxBufferMut;

/// Mutable receive buffer type delivered by a UDP socket.
pub type UdpRxBuffer<S> = <S as UdpSocket>::RxBuffer;

/// High-level UDP socket interface.
pub trait UdpSocket
where
    <Self::RxBuffer as PacketBufferMut>::Frozen: Send,
    <Self::TxBufferMut as PacketBufferMut>::Frozen: Send,
{
    /// Mutable buffer type delivered by the socket receive path.
    type RxBuffer: PacketBufferMut + Send;

    /// Mutable buffer type allocated by the socket transmit path.
    type TxBufferMut: PacketBufferMut + Send;

    /// Polling driver selected by this socket implementation.
    type Driver: PollDriver;

    /// Receive metadata type delivered by this socket.
    type RecvMeta;

    /// Returns the logical identity of this socket.
    fn socket_id(&self) -> SocketId;

    /// Returns the maximum UDP payload length accepted on transmit.
    fn mtu(&self) -> usize;

    /// Returns the CPU(s) a worker owning this socket should pin to.
    ///
    /// Defaults to [`QueueAffinity::Any`] (no hint). Implementations that know
    /// their target core can override this.
    fn worker_affinity(&self) -> QueueAffinity {
        QueueAffinity::Any
    }

    /// Returns UDP socket capabilities.
    fn capabilities(&self) -> UdpCapabilities {
        UdpCapabilities::default()
    }

    /// Allocates up to `max` socket-owned transmit buffers into `out`.
    fn allocate_tx_batch(
        &mut self,
        out: &mut Vec<UdpTxBufferMut<Self>>,
        max: usize,
    ) -> Result<usize, Error>
    where
        Self: Sized;

    /// Returns the polling driver.
    fn driver(&self) -> &Self::Driver;

    /// Returns the polling driver mutably.
    fn driver_mut(&mut self) -> &mut Self::Driver;

    /// Sends a batch of UDP packets, consuming accepted slots in order.
    fn send(
        &mut self,
        batch: &mut [TxSlot<UdpTransmit<UdpTxBuffer<Self>>>],
    ) -> Result<usize, SendError>;

    /// Sends `batch` to completion, draining TX completions on partial
    /// acceptance so the next `send` has transmit capacity.
    ///
    /// The default implementation loops `send` + `drain_tx_completions` until
    /// every slot is taken or a non-back-pressure error fires.
    fn send_all(
        &mut self,
        batch: &mut [TxSlot<UdpTransmit<UdpTxBuffer<Self>>>],
    ) -> Result<usize, SendError>
    where
        Self: Sized,
    {
        let mut total = 0usize;
        while total < batch.len() {
            match self.send(&mut batch[total..]) {
                Ok(0) => {
                    if let Err(error) = self.drain_tx_completions() {
                        return Err(SendError {
                            accepted: total,
                            kind: error,
                        });
                    }
                    core::hint::spin_loop();
                }
                Ok(n) => total += n,
                Err(SendError { accepted, kind }) => {
                    return Err(SendError {
                        accepted: total + accepted,
                        kind,
                    });
                }
            }
        }
        Ok(total)
    }

    /// Receives a batch of UDP packets into `out`.
    fn recv(
        &mut self,
        out: &mut RecvBatch<UdpReceive<UdpRxBuffer<Self>, Self::RecvMeta>>,
    ) -> Result<usize, Error>;

    /// Drains transmit completions and reclaims socket-owned buffers.
    fn drain_tx_completions(&mut self) -> Result<usize, Error>;

    /// Notifies the transmit path when an explicit flush is required.
    fn notify_tx(&mut self) -> Result<(), Error> {
        Ok(())
    }
}
