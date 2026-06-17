//! IP packet socket traits and packet metadata.

use core::num::NonZeroU16;

use crate::route::{NeighborId, RouteId};
use crate::{
    Error, IpFamily, Mixed, PacketBufferMut, PollDriver, QueueAffinity, RecvBatch, SendError,
    SocketId, TxSlot,
};

/// IP version carried by an IP packet.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum IpVersion {
    /// IPv4 datagram.
    V4,
    /// IPv6 datagram.
    V6,
}

/// Receive checksum/offload status.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ChecksumStatus {
    /// The L4 checksum was verified and the packet is good.
    Verified,
    /// The checksum was verified and is wrong.
    Bad,
    /// Partial checksum information needs interpretation.
    Unverified,
    /// No checksum status was provided.
    NotChecked,
}

/// Default IP packet receive metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IpPacketRecvMeta {
    /// IP version for mixed-family sockets.
    pub version: IpVersion,
    /// Complete IP datagram length.
    pub len: usize,
    /// L4 checksum status when known.
    pub checksum: ChecksumStatus,
}

/// Optional transmit checksum offload flags.
///
/// TCP/UDP segmentation offload (TSO) is represented by
/// [`IpPacketTransmit::tso_segment_size`].
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct TxOffload(u32);

impl TxOffload {
    /// No transmit offloads requested.
    pub const NONE: Self = Self(0);
    /// Request IPv4 header checksum offload.
    pub const CKSUM_IP: Self = Self(1 << 0);
    /// Request L4 checksum offload.
    pub const CKSUM_L4: Self = Self(1 << 1);

    /// Creates flags from raw bits.
    #[must_use]
    pub const fn from_bits(bits: u32) -> Self {
        Self(bits)
    }

    /// Returns the raw bit representation.
    #[must_use]
    pub const fn bits(self) -> u32 {
        self.0
    }

    /// Returns `true` when all `flags` are present.
    #[must_use]
    pub const fn contains(self, flags: Self) -> bool {
        (self.0 & flags.0) == flags.0
    }

    /// Returns these flags plus `flags`.
    #[must_use]
    pub const fn union(self, flags: Self) -> Self {
        Self(self.0 | flags.0)
    }
}

impl core::ops::BitOr for TxOffload {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        self.union(rhs)
    }
}

impl core::ops::BitOrAssign for TxOffload {
    fn bitor_assign(&mut self, rhs: Self) {
        *self = self.union(rhs);
    }
}

/// Egress handle consumed by IP packet sends.
///
/// Implementations choose an egress type that matches their routing model. The
/// trait carries no methods.
pub trait IpPacketEgress: Copy + 'static {}

impl IpPacketEgress for () {}

/// Core egress handle for implementations that do not need custom variants.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum CoreEgress {
    /// Use the queue's configured default egress.
    #[default]
    Default,
    /// Use a route handle.
    Route(RouteId),
    /// Use a neighbor handle.
    Neighbor(NeighborId),
}

impl IpPacketEgress for CoreEgress {}

/// One complete IP datagram received by an IP packet socket.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IpPacketReceive<B, M = IpPacketRecvMeta> {
    /// Complete IP datagram bytes.
    pub packet: B,
    /// Receive metadata.
    pub meta: M,
}

impl<B, M> IpPacketReceive<B, M> {
    /// Creates an IP packet receive item from packet bytes and metadata.
    #[must_use]
    pub const fn new(packet: B, meta: M) -> Self {
        Self { packet, meta }
    }
}

/// One complete IP datagram submitted for transmit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IpPacketTransmit<B, E, F: IpFamily = Mixed> {
    /// Complete IPv4 or IPv6 datagram bytes.
    pub packet: B,
    /// Resolved egress handle.
    pub egress: E,
    /// Optional parsed source-address hint.
    pub source: Option<F::Addr>,
    /// Optional parsed destination-address hint.
    pub destination: Option<F::Addr>,
    /// Optional TTL/hop-limit override or preservation hint.
    pub hop_limit: Option<u8>,
    /// Per-packet transmit offload requests (checksum offloads only — see
    /// `tso_segment_size` for TSO).
    pub offload: TxOffload,
    /// Hardware segmentation: `Some(size)` enables TSO with the given
    /// segment size; `None` disables it. This single field is both the
    /// enable bit and the parameter, so the two can't disagree.
    pub tso_segment_size: Option<NonZeroU16>,
}

impl<B, E, F> IpPacketTransmit<B, E, F>
where
    F: IpFamily,
{
    /// Creates an IP packet transmit item from packet bytes and an egress handle.
    #[must_use]
    pub const fn new(packet: B, egress: E) -> Self {
        Self {
            packet,
            egress,
            source: None,
            destination: None,
            hop_limit: None,
            offload: TxOffload::NONE,
            tso_segment_size: None,
        }
    }
}

/// Immutable transmit buffer type derived from an IP packet socket's transmit buffer type.
pub type IpPacketTxBuffer<S> = <<S as IpPacketSocket>::TxBufferMut as PacketBufferMut>::Frozen;

/// Mutable transmit buffer type allocated by an IP packet socket.
pub type IpPacketTxBufferMut<S> = <S as IpPacketSocket>::TxBufferMut;

/// Mutable receive buffer type delivered by an IP packet socket.
pub type IpPacketRxBuffer<S> = <S as IpPacketSocket>::RxBuffer;

/// Transmit item type derived from an IP packet socket's pools and egress family.
pub type IpPacketTxItem<S> = IpPacketTransmit<
    IpPacketTxBuffer<S>,
    <S as IpPacketSocket>::Egress,
    <S as IpPacketSocket>::Family,
>;

/// Receive item type derived from an IP packet socket's receive pool and metadata.
pub type IpPacketRxItem<S> = IpPacketReceive<IpPacketRxBuffer<S>, <S as IpPacketSocket>::RecvMeta>;

/// IP-packet queue abstraction.
pub trait IpPacketSocket
where
    <Self::RxBuffer as PacketBufferMut>::Frozen: Send,
    <Self::TxBufferMut as PacketBufferMut>::Frozen: Send,
{
    /// Mutable buffer type delivered by the socket receive path.
    type RxBuffer: PacketBufferMut + Send;

    /// Mutable buffer type allocated by the socket transmit path.
    type TxBufferMut: PacketBufferMut + Send;

    /// Compile-time address-family policy.
    type Family: IpFamily;

    /// Egress handle consumed by IP packet sends.
    type Egress: IpPacketEgress;

    /// Polling driver selected by this socket implementation.
    type Driver: PollDriver;

    /// Receive metadata type delivered by this socket.
    type RecvMeta;

    /// Returns the logical identity of this socket.
    fn socket_id(&self) -> SocketId;

    /// Returns the IP-layer MTU for complete datagrams passed across this trait.
    fn mtu(&self) -> usize;

    /// Returns the CPU(s) a worker owning this socket should pin to.
    ///
    /// Defaults to [`QueueAffinity::Any`] (no hint). Implementations that know
    /// their target core can override this.
    fn worker_affinity(&self) -> QueueAffinity {
        QueueAffinity::Any
    }

    /// Returns the polling driver.
    fn driver(&self) -> &Self::Driver;

    /// Returns the polling driver mutably.
    fn driver_mut(&mut self) -> &mut Self::Driver;

    /// Allocates up to `max` socket-owned transmit buffers into `out`.
    fn allocate_tx_batch(
        &mut self,
        out: &mut Vec<IpPacketTxBufferMut<Self>>,
        max: usize,
    ) -> Result<usize, Error>
    where
        Self: Sized;

    /// Sends a batch of complete IP datagrams, consuming accepted slots in order.
    fn send(&mut self, batch: &mut [TxSlot<IpPacketTxItem<Self>>]) -> Result<usize, SendError>;

    /// Sends `batch` to completion, draining TX completions on partial
    /// acceptance so the next `send` has transmit capacity.
    fn send_all(&mut self, batch: &mut [TxSlot<IpPacketTxItem<Self>>]) -> Result<usize, SendError>
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

    /// Receives a batch of complete IP datagrams into `out`.
    fn recv(&mut self, out: &mut RecvBatch<IpPacketRxItem<Self>>) -> Result<usize, Error>;

    /// Drains transmit completions and reclaims socket-owned buffers.
    fn drain_tx_completions(&mut self) -> Result<usize, Error>;

    /// Notifies the transmit path when an explicit flush is required.
    fn notify_tx(&mut self) -> Result<(), Error> {
        Ok(())
    }
}
