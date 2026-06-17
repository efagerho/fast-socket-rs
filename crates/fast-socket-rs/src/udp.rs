//! UDP-facing socket traits and packet metadata.

use core::num::NonZeroU16;
use std::net::{IpAddr, SocketAddr};

use crate::{
    Error, PacketBuffer, PacketBufferMut, PollDriver, QueueAffinity, RecvBatch, SendError,
    SocketId, TxSlot,
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

/// Request used to prepare a socket-specific UDP endpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UdpEndpointSpec {
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
    /// Optional fixed UDP payload length.
    pub payload_len: Option<usize>,
}

impl UdpEndpointSpec {
    /// Creates a variable-length endpoint request for a destination address.
    #[must_use]
    pub const fn new(destination: SocketAddr) -> Self {
        Self {
            destination,
            source_ip: None,
            source_port: None,
            ecn: None,
            gso_segment_size: None,
            payload_len: None,
        }
    }
}

/// One UDP payload submitted through a prepared endpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UdpEndpointTransmit<B> {
    /// Packet payload buffer.
    pub packet: B,
}

impl<B> UdpEndpointTransmit<B> {
    /// Creates an endpoint transmit item.
    #[must_use]
    pub const fn new(packet: B) -> Self {
        Self { packet }
    }
}

/// Prepared UDP endpoint limits and offload information.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UdpEndpointInfo {
    /// Maximum UDP payload length accepted without segmentation.
    pub mtu: usize,
    /// Fixed payload length, when the endpoint was prepared for one.
    pub payload_len: Option<usize>,
    /// UDP segmentation size, when configured for the endpoint.
    pub gso_segment_size: Option<NonZeroU16>,
}

/// Generic prepared UDP endpoint used by backends without a specialized path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GenericUdpEndpoint {
    spec: UdpEndpointSpec,
    info: UdpEndpointInfo,
}

impl GenericUdpEndpoint {
    /// Returns the original endpoint request.
    #[must_use]
    pub const fn spec(&self) -> &UdpEndpointSpec {
        &self.spec
    }

    /// Returns endpoint limits and offload information.
    #[must_use]
    pub const fn info(&self) -> UdpEndpointInfo {
        self.info
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

/// Prepares a correctness-first generic UDP endpoint for `socket`.
///
/// Backends can use this as their mandatory endpoint implementation while
/// reserving specialized endpoint handles for later optimization.
pub fn prepare_generic_udp_endpoint<S>(
    socket: &S,
    spec: UdpEndpointSpec,
) -> Result<GenericUdpEndpoint, Error>
where
    S: UdpSocket,
{
    let info = validate_udp_endpoint_spec(socket.mtu(), socket.capabilities(), &spec)?;
    Ok(GenericUdpEndpoint { spec, info })
}

/// Sends a batch through a generic endpoint by delegating to [`UdpSocket::send`].
///
/// Accepted endpoint slots are consumed in order. If the delegated send path
/// accepts only a prefix, unaccepted packets are restored to their original
/// endpoint slots.
pub fn send_generic_udp_endpoint<S>(
    socket: &mut S,
    endpoint: &mut GenericUdpEndpoint,
    batch: &mut [TxSlot<UdpEndpointTransmit<UdpTxBuffer<S>>>],
) -> Result<usize, SendError>
where
    S: UdpSocket,
{
    let mut prefix_len = 0usize;
    let mut deferred_error = None;
    while prefix_len < batch.len() {
        let Some(tx) = batch[prefix_len].as_ref() else {
            deferred_error = Some(Error::InvalidBatch);
            break;
        };
        if let Err(error) = validate_udp_endpoint_packet(endpoint.info, tx.packet.len()) {
            deferred_error = Some(error);
            break;
        }
        prefix_len += 1;
    }

    if prefix_len == 0 {
        return match deferred_error {
            Some(kind) => Err(SendError { accepted: 0, kind }),
            None => Ok(0),
        };
    }

    let mut converted = Vec::with_capacity(prefix_len);
    for slot in batch.iter_mut().take(prefix_len) {
        let tx = slot.take().expect("validated ready endpoint slot");
        converted.push(TxSlot::Ready(UdpTransmit {
            packet: tx.packet,
            destination: endpoint.spec.destination,
            source_ip: endpoint.spec.source_ip,
            source_port: endpoint.spec.source_port,
            ecn: endpoint.spec.ecn,
            gso_segment_size: endpoint.spec.gso_segment_size,
        }));
    }

    match socket.send(&mut converted) {
        Ok(accepted) => {
            restore_unaccepted_endpoint_slots::<S>(batch, &mut converted, accepted);
            if accepted < prefix_len {
                return Ok(accepted);
            }
            match deferred_error {
                Some(kind) => Err(SendError {
                    accepted: prefix_len,
                    kind,
                }),
                None => Ok(prefix_len),
            }
        }
        Err(SendError { accepted, kind }) => {
            restore_unaccepted_endpoint_slots::<S>(batch, &mut converted, accepted);
            Err(SendError { accepted, kind })
        }
    }
}

fn validate_udp_endpoint_spec(
    mtu: usize,
    capabilities: UdpCapabilities,
    spec: &UdpEndpointSpec,
) -> Result<UdpEndpointInfo, Error> {
    if let Some(source) = spec.source_ip
        && !same_ip_family(source, spec.destination.ip())
    {
        return Err(Error::InvalidPacket);
    }

    if let Some(segment_size) = spec.gso_segment_size {
        if !capabilities.gso {
            return Err(Error::InvalidPacket);
        }
        if usize::from(segment_size.get()) > mtu {
            return Err(Error::OversizeForMtu);
        }
        if let (Some(payload_len), Some(max_segments)) =
            (spec.payload_len, capabilities.max_gso_segments)
        {
            let segments = payload_len.div_ceil(usize::from(segment_size.get()));
            if segments > usize::from(max_segments.get()) {
                return Err(Error::OversizeForMtu);
            }
        }
    } else if let Some(payload_len) = spec.payload_len
        && payload_len > mtu
    {
        return Err(Error::OversizeForMtu);
    }

    Ok(UdpEndpointInfo {
        mtu,
        payload_len: spec.payload_len,
        gso_segment_size: spec.gso_segment_size,
    })
}

fn validate_udp_endpoint_packet(info: UdpEndpointInfo, payload_len: usize) -> Result<(), Error> {
    if let Some(expected) = info.payload_len
        && payload_len != expected
    {
        return Err(Error::InvalidPacket);
    }
    if info.gso_segment_size.is_none() && payload_len > info.mtu {
        return Err(Error::OversizeForMtu);
    }
    Ok(())
}

fn same_ip_family(left: IpAddr, right: IpAddr) -> bool {
    matches!(
        (left, right),
        (IpAddr::V4(_), IpAddr::V4(_)) | (IpAddr::V6(_), IpAddr::V6(_))
    )
}

fn restore_unaccepted_endpoint_slots<S>(
    batch: &mut [TxSlot<UdpEndpointTransmit<UdpTxBuffer<S>>>],
    converted: &mut [TxSlot<UdpTransmit<UdpTxBuffer<S>>>],
    accepted: usize,
) where
    S: UdpSocket,
{
    for (slot, converted) in batch.iter_mut().zip(converted.iter_mut()).skip(accepted) {
        if let Some(tx) = converted.take() {
            *slot = TxSlot::Ready(UdpEndpointTransmit::new(tx.packet));
        }
    }
}

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

    /// Socket-specific prepared UDP endpoint handle.
    type Endpoint;

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

    /// Prepares a socket-owned transmit plan for one UDP metadata shape.
    fn prepare_udp_endpoint(&mut self, spec: UdpEndpointSpec) -> Result<Self::Endpoint, Error>;

    /// Returns the original endpoint request used to build this handle.
    fn udp_endpoint_spec<'a>(&self, endpoint: &'a Self::Endpoint) -> &'a UdpEndpointSpec;

    /// Returns endpoint limits and offload information.
    fn udp_endpoint_info(&self, endpoint: &Self::Endpoint) -> UdpEndpointInfo;

    /// Sends a batch through a prepared endpoint, consuming accepted slots in
    /// order.
    fn send_to_udp_endpoint(
        &mut self,
        endpoint: &mut Self::Endpoint,
        batch: &mut [TxSlot<UdpEndpointTransmit<UdpTxBuffer<Self>>>],
    ) -> Result<usize, SendError>
    where
        Self: Sized;

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

    /// Sends `batch` through `endpoint` to completion, draining TX completions
    /// on partial acceptance so the next endpoint send has transmit capacity.
    fn send_all_to_udp_endpoint(
        &mut self,
        endpoint: &mut Self::Endpoint,
        batch: &mut [TxSlot<UdpEndpointTransmit<UdpTxBuffer<Self>>>],
    ) -> Result<usize, SendError>
    where
        Self: Sized,
    {
        let mut total = 0usize;
        while total < batch.len() {
            match self.send_to_udp_endpoint(endpoint, &mut batch[total..]) {
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
