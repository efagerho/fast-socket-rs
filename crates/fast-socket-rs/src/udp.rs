//! UDP-facing socket traits and packet metadata.

use core::num::NonZeroU16;
use std::net::{IpAddr, SocketAddr};

use crate::{
    BufferPool, Error, PacketBufferMut, PollDriver, QueueAffinity, RecvBatch, SendError, SocketId,
    TxSlot,
};
use crate::{BusyPollDriverMode, ReadinessDriverMode};

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
    /// Local destination IP when the backend exposes it.
    pub destination: Option<IpAddr>,
    /// ECN codepoint when the backend exposes it.
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

/// Immutable transmit buffer type derived from a UDP socket's transmit pool.
pub type UdpTxBuffer<S> =
    <<<S as UdpSocket>::TxPool as BufferPool>::Buffer as PacketBufferMut>::Frozen;

/// Mutable transmit buffer type derived from a UDP socket's transmit pool.
pub type UdpTxBufferMut<S> = <<S as UdpSocket>::TxPool as BufferPool>::Buffer;

/// Mutable receive buffer type derived from a UDP socket's receive pool.
pub type UdpRxBuffer<S> = <<S as UdpSocket>::RxPool as BufferPool>::Buffer;

/// High-level UDP socket interface.
pub trait UdpSocket {
    /// Buffer pool used by the socket receive path.
    type RxPool: BufferPool;

    /// Buffer pool used by the socket transmit path.
    type TxPool: BufferPool;

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

    /// Returns the maximum UDP payload length accepted on transmit.
    fn mtu(&self) -> usize;

    /// Returns the CPU(s) a worker owning this socket should pin to.
    ///
    /// Defaults to [`QueueAffinity::Any`] (no hint). Backends that know their
    /// target core override this; see
    /// [`pin_current_thread_to_socket`](crate::pin_current_thread_to_socket).
    fn worker_affinity(&self) -> QueueAffinity {
        QueueAffinity::Any
    }

    /// Returns UDP socket capabilities.
    fn capabilities(&self) -> UdpCapabilities {
        UdpCapabilities::default()
    }

    /// Returns the socket-owned receive buffer pool.
    fn rx_pool(&self) -> &Self::RxPool;

    /// Returns the socket-owned receive buffer pool mutably.
    fn rx_pool_mut(&mut self) -> &mut Self::RxPool;

    /// Returns the socket-owned transmit buffer pool.
    fn tx_pool(&self) -> &Self::TxPool;

    /// Returns the socket-owned transmit buffer pool mutably.
    fn tx_pool_mut(&mut self) -> &mut Self::TxPool;

    /// Allocates up to `max` socket-owned transmit buffers into `out`.
    ///
    /// The default implementation allocates from the transmit pool and, when
    /// the pool is empty, drains transmit completions once before retrying.
    fn allocate_tx_batch(
        &mut self,
        out: &mut Vec<UdpTxBufferMut<Self>>,
        max: usize,
    ) -> Result<usize, Error>
    where
        Self: Sized,
    {
        let start_len = out.len();
        let mut drained_after_empty = false;
        while out.len() - start_len < max {
            if let Some(buffer) = self.tx_pool_mut().allocate() {
                out.push(buffer);
                drained_after_empty = false;
                continue;
            }

            if drained_after_empty {
                break;
            }

            if self.drain_tx_completions()? == 0 {
                break;
            }
            drained_after_empty = true;
        }
        Ok(out.len() - start_len)
    }

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
    /// acceptance so the next `send` has free ring slots.
    ///
    /// The default implementation loops `send` + `drain_tx_completions` until
    /// every slot is taken or a non-back-pressure error fires. Backends that
    /// can do better (e.g., notify-on-drain without re-running `send`) may
    /// override this.
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

    /// Notifies the transmit path when a backend requires an explicit doorbell.
    fn notify_tx(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

/// Marker trait for readiness-mode UDP sockets.
pub trait ReadinessUdpSocket: UdpSocket + sealed::ReadinessUdpSocketSealed {}

impl<S> ReadinessUdpSocket for S where S: UdpSocket + sealed::ReadinessUdpSocketSealed {}

/// Marker trait for busy-poll-mode UDP sockets.
pub trait BusyPollUdpSocket: UdpSocket + sealed::BusyPollUdpSocketSealed {}

impl<S> BusyPollUdpSocket for S where S: UdpSocket + sealed::BusyPollUdpSocketSealed {}

mod sealed {
    use super::*;

    pub trait ReadinessUdpSocketSealed {}

    impl<S> ReadinessUdpSocketSealed for S
    where
        S: UdpSocket,
        S::Driver: ReadinessDriverMode,
    {
    }

    pub trait BusyPollUdpSocketSealed {}

    impl<S> BusyPollUdpSocketSealed for S
    where
        S: UdpSocket,
        S::Driver: BusyPollDriverMode,
    {
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddrV4};

    use crate::{BusyPollDriver, HeapBufferPool, PacketBuf, PacketBufMut};

    use super::*;

    struct MockUdpSocket {
        rx_pool: HeapBufferPool,
        tx_pool: HeapBufferPool,
        driver: BusyPollDriver,
        sent: usize,
    }

    impl MockUdpSocket {
        fn new() -> Self {
            Self {
                rx_pool: HeapBufferPool::with_payload_capacity(128),
                tx_pool: HeapBufferPool::with_payload_capacity(128),
                driver: BusyPollDriver::new(),
                sent: 0,
            }
        }
    }

    impl UdpSocket for MockUdpSocket {
        type RxPool = HeapBufferPool;
        type TxPool = HeapBufferPool;
        type Driver = BusyPollDriver;
        type RecvMeta = UdpRecvMeta;

        fn socket_id(&self) -> SocketId {
            SocketId::new(7)
        }

        fn mtu(&self) -> usize {
            128
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
            batch: &mut [TxSlot<UdpTransmit<PacketBuf>>],
        ) -> Result<usize, SendError> {
            for slot in batch.iter_mut() {
                let Some(_item) = slot.take() else {
                    return Err(SendError {
                        accepted: self.sent,
                        kind: Error::InvalidBatch,
                    });
                };
                self.sent += 1;
            }
            Ok(batch.len())
        }

        fn recv(
            &mut self,
            out: &mut RecvBatch<UdpReceive<PacketBufMut, Self::RecvMeta>>,
        ) -> Result<usize, Error> {
            let meta = UdpRecvMeta {
                source: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1234).into(),
                destination: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
                ecn: None,
                len: 0,
                gro_stride: None,
            };
            let packet = self.rx_pool.allocate().ok_or(Error::WouldBlock)?;
            out.push(UdpReceive::new(packet, meta))
                .map_err(|_| Error::BatchFull)?;
            Ok(1)
        }

        fn drain_tx_completions(&mut self) -> Result<usize, Error> {
            Ok(0)
        }
    }

    fn assert_busy_poll_udp_socket<S: BusyPollUdpSocket>(_socket: &S) {}

    #[test]
    fn udp_socket_trait_surface_accepts_mock_backend() {
        let mut socket = MockUdpSocket::new();
        assert_busy_poll_udp_socket(&socket);
        assert_eq!(socket.socket_id(), SocketId::new(7));

        let destination = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9999).into();
        let packet = PacketBufMut::copy_from_slice(b"ping").freeze();
        let mut tx = [TxSlot::Ready(UdpTransmit::new(packet, destination))];
        assert_eq!(socket.send(&mut tx).unwrap(), 1);
        assert!(tx[0].is_taken());

        let mut rx = RecvBatch::with_capacity(4);
        assert_eq!(socket.recv(&mut rx).unwrap(), 1);
        assert_eq!(rx.len(), 1);
    }
}
