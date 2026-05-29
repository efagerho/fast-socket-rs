//! IP packet socket traits and packet metadata.

use core::num::NonZeroU16;

use crate::route::{NeighborId, RouteId};
use crate::{
    BufferPool, BusyPollDriverMode, Error, IpFamily, Mixed, PacketBufferMut, PollDriver,
    QueueAffinity, ReadinessDriverMode, RecvBatch, SendError, SocketId, TxSlot,
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
    /// NIC verified the L4 checksum and the packet is good.
    Verified,
    /// NIC verified the checksum and it is wrong.
    Bad,
    /// NIC computed partial checksum information that needs interpretation.
    Unverified,
    /// Backend did not check the checksum.
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

/// Optional transmit offload flags.
///
/// TCP/UDP segmentation offload (TSO) is **not** represented here — set
/// [`IpPacketTransmit::tso_segment_size`] to `Some(...)` to request it.
/// Pairing a separate `TxOffload::TSO` flag with the segment-size field was
/// removed because the two could disagree (flag set but size missing, or
/// vice versa); keeping the segment size as the single source of truth makes
/// the invariant `tso_enabled <=> tso_segment_size.is_some()` impossible to
/// violate.
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

/// Backend-specific egress handle consumed by IP packet sends.
///
/// Backends may add their own variants; the trait carries no methods. The
/// previous `default_egress()` hook was removed in favor of explicit defaults
/// at the call site: AF_XDP requires a real interface/queue and `()` /
/// [`CoreEgress`] both have an obvious zero value, so the indirection was
/// dead weight.
pub trait IpPacketEgress: Copy + 'static {}

impl IpPacketEgress for () {}

/// Core egress handle for backends that do not need custom variants.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum CoreEgress {
    /// Use the queue's configured default egress.
    #[default]
    Default,
    /// Use a route-table handle.
    Route(RouteId),
    /// Use a neighbor-table handle.
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
    /// Fully resolved backend egress handle.
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

/// Immutable transmit buffer type derived from an IP packet socket's transmit pool.
pub type IpPacketTxBuffer<S> =
    <<<S as IpPacketSocket>::TxPool as BufferPool>::Buffer as PacketBufferMut>::Frozen;

/// Mutable receive buffer type derived from an IP packet socket's receive pool.
pub type IpPacketRxBuffer<S> = <<S as IpPacketSocket>::RxPool as BufferPool>::Buffer;

/// Transmit item type derived from an IP packet socket's pools and egress family.
pub type IpPacketTxItem<S> = IpPacketTransmit<
    IpPacketTxBuffer<S>,
    <S as IpPacketSocket>::Egress,
    <S as IpPacketSocket>::Family,
>;

/// Receive item type derived from an IP packet socket's receive pool and metadata.
pub type IpPacketRxItem<S> = IpPacketReceive<IpPacketRxBuffer<S>, <S as IpPacketSocket>::RecvMeta>;

/// IP-packet queue abstraction for forwarding and kernel-bypass backends.
pub trait IpPacketSocket {
    /// Buffer pool used by the socket receive path.
    type RxPool: BufferPool;

    /// Buffer pool used by the socket transmit path.
    type TxPool: BufferPool;

    /// Compile-time address-family policy.
    type Family: IpFamily;

    /// Backend-specific egress handle consumed by IP packet sends.
    type Egress: IpPacketEgress;

    /// Polling driver selected by this socket implementation.
    type Driver: PollDriver;

    /// Receive metadata type delivered by this socket.
    type RecvMeta;

    /// Returns the logical identity of this socket.
    ///
    /// Unique among the sockets a factory hands out. The backing NIC queues are
    /// reported by [`RawDevice::nic_queues`](crate::RawDevice::nic_queues), not
    /// this method.
    fn socket_id(&self) -> SocketId;

    /// Returns the IP-layer MTU for complete datagrams passed across this trait.
    fn mtu(&self) -> usize;

    /// Returns the CPU(s) a worker owning this socket should pin to.
    ///
    /// Defaults to [`QueueAffinity::Any`] (no hint). Backends that know their
    /// target core override this; see
    /// [`pin_current_thread_to_ip_packet_socket`](crate::pin_current_thread_to_ip_packet_socket).
    fn worker_affinity(&self) -> QueueAffinity {
        QueueAffinity::Any
    }

    /// Returns the socket-owned receive buffer pool.
    fn rx_pool(&self) -> &Self::RxPool;

    /// Returns the socket-owned receive buffer pool mutably.
    fn rx_pool_mut(&mut self) -> &mut Self::RxPool;

    /// Returns the socket-owned transmit buffer pool.
    fn tx_pool(&self) -> &Self::TxPool;

    /// Returns the socket-owned transmit buffer pool mutably.
    fn tx_pool_mut(&mut self) -> &mut Self::TxPool;

    /// Returns the polling driver.
    fn driver(&self) -> &Self::Driver;

    /// Returns the polling driver mutably.
    fn driver_mut(&mut self) -> &mut Self::Driver;

    /// Sends a batch of complete IP datagrams, consuming accepted slots in order.
    fn send(&mut self, batch: &mut [TxSlot<IpPacketTxItem<Self>>]) -> Result<usize, SendError>;

    /// Receives a batch of complete IP datagrams into `out`.
    fn recv(&mut self, out: &mut RecvBatch<IpPacketRxItem<Self>>) -> Result<usize, Error>;

    /// Drains transmit completions and reclaims socket-owned buffers.
    fn drain_tx_completions(&mut self) -> Result<usize, Error>;

    /// Notifies the transmit path when a backend requires an explicit doorbell.
    fn notify_tx(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

/// Marker trait for readiness-mode IP packet sockets.
pub trait ReadinessIpPacketSocket: IpPacketSocket + sealed::ReadinessIpPacketSocketSealed {}

impl<S> ReadinessIpPacketSocket for S where S: IpPacketSocket + sealed::ReadinessIpPacketSocketSealed
{}

/// Marker trait for busy-poll-mode IP packet sockets.
pub trait BusyPollIpPacketSocket: IpPacketSocket + sealed::BusyPollIpPacketSocketSealed {}

impl<S> BusyPollIpPacketSocket for S where S: IpPacketSocket + sealed::BusyPollIpPacketSocketSealed {}

mod sealed {
    use super::*;

    pub trait ReadinessIpPacketSocketSealed {}

    impl<S> ReadinessIpPacketSocketSealed for S
    where
        S: IpPacketSocket,
        S::Driver: ReadinessDriverMode,
    {
    }

    pub trait BusyPollIpPacketSocketSealed {}

    impl<S> BusyPollIpPacketSocketSealed for S
    where
        S: IpPacketSocket,
        S::Driver: BusyPollDriverMode,
    {
    }
}

#[cfg(test)]
mod tests {
    use crate::{BusyPollDriver, HeapBufferPool, PacketBuf, PacketBufMut, V4Only};

    use super::*;

    struct MockIpPacketSocket {
        rx_pool: HeapBufferPool,
        tx_pool: HeapBufferPool,
        driver: BusyPollDriver,
    }

    impl MockIpPacketSocket {
        fn new() -> Self {
            Self {
                rx_pool: HeapBufferPool::with_payload_capacity(1500),
                tx_pool: HeapBufferPool::with_payload_capacity(1500),
                driver: BusyPollDriver::new(),
            }
        }
    }

    impl IpPacketSocket for MockIpPacketSocket {
        type RxPool = HeapBufferPool;
        type TxPool = HeapBufferPool;
        type Family = V4Only;
        type Egress = CoreEgress;
        type Driver = BusyPollDriver;
        type RecvMeta = IpPacketRecvMeta;

        fn socket_id(&self) -> SocketId {
            SocketId::new(1)
        }

        fn mtu(&self) -> usize {
            1500
        }

        fn rx_pool(&self) -> &Self::RxPool {
            &self.rx_pool
        }

        fn rx_pool_mut(&mut self) -> &mut Self::RxPool {
            &mut self.rx_pool
        }

        fn tx_pool(&self) -> &Self::TxPool {
            &self.tx_pool
        }

        fn tx_pool_mut(&mut self) -> &mut Self::TxPool {
            &mut self.tx_pool
        }

        fn driver(&self) -> &Self::Driver {
            &self.driver
        }

        fn driver_mut(&mut self) -> &mut Self::Driver {
            &mut self.driver
        }

        fn send(
            &mut self,
            batch: &mut [TxSlot<IpPacketTransmit<PacketBuf, Self::Egress, Self::Family>>],
        ) -> Result<usize, SendError> {
            for (accepted, slot) in batch.iter_mut().enumerate() {
                if slot.take().is_none() {
                    return Err(SendError {
                        accepted,
                        kind: Error::InvalidBatch,
                    });
                }
            }
            Ok(batch.len())
        }

        fn recv(
            &mut self,
            out: &mut RecvBatch<IpPacketReceive<PacketBufMut, Self::RecvMeta>>,
        ) -> Result<usize, Error> {
            let packet = self.rx_pool.allocate().ok_or(Error::WouldBlock)?;
            out.push(IpPacketReceive::new(
                packet,
                IpPacketRecvMeta {
                    version: IpVersion::V4,
                    len: 0,
                    checksum: ChecksumStatus::NotChecked,
                },
            ))
            .map_err(|_| Error::BatchFull)?;
            Ok(1)
        }

        fn drain_tx_completions(&mut self) -> Result<usize, Error> {
            Ok(0)
        }
    }

    fn assert_busy_poll_ip_packet_socket<S: BusyPollIpPacketSocket>(_socket: &S) {}

    #[test]
    fn ip_packet_socket_trait_surface_accepts_mock_backend() {
        let mut socket = MockIpPacketSocket::new();
        assert_busy_poll_ip_packet_socket(&socket);

        let packet = PacketBufMut::copy_from_slice(&[0x45, 0, 0, 20]).freeze();
        let mut tx = [TxSlot::Ready(IpPacketTransmit::new(
            packet,
            CoreEgress::Neighbor(NeighborId::new(1)),
        ))];
        assert_eq!(socket.send(&mut tx).unwrap(), 1);
        assert!(tx[0].is_taken());

        let mut rx = RecvBatch::with_capacity(1);
        assert_eq!(socket.recv(&mut rx).unwrap(), 1);
        assert_eq!(rx.as_slice()[0].meta.version, IpVersion::V4);
    }

    #[test]
    fn tx_offload_flags_compose_without_dependency() {
        let flags = TxOffload::CKSUM_IP | TxOffload::CKSUM_L4;
        assert!(flags.contains(TxOffload::CKSUM_IP));
        assert!(flags.contains(TxOffload::CKSUM_L4));
    }
}
