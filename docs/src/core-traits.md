# Core Traits

The core traits describe packet memory, backend-owned socket pools, batch
ownership, polling, routing, and the socket APIs that move packets between an
application and a backend.

This chapter is a map of the current public surface. Backend-specific builders
and async integration layers are covered in later chapters.

The trait snippets below are intentionally high-level: they omit default
implementations and `where` clauses when those bounds are API plumbing rather
than the behavior being documented.

## Buffers

Avoiding packet copies is central to the fast socket model. On the receive path,
the application gets direct access to memory filled by the backend. On the send
path, the application builds packet bytes in socket-owned buffers that can be
handed back to the backend for transmission.

Two traits describe those packet buffers:

- `PacketBuffer`, the immutable packet view accepted by transmit paths;
- `PacketBufferMut`, the mutable packet view delivered by receive paths and
  allocated for transmit construction.

When a socket receives a packet, it delivers a mutable `PacketBufferMut`. When an
application sends a packet, it submits the immutable frozen form of a mutable
buffer. Freezing prevents later mutation while the backend or device may still
read the packet bytes.

A mutable `PacketBufferMut` can be frozen into its associated immutable
`PacketBuffer`. This supports zero-copy forwarding: a worker can edit headers in
place, freeze the buffer, and submit it to another transmit path.

```rust,ignore
pub trait PacketBuffer {
    type Segments<'a>: Iterator<Item = Segment<'a>>;

    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;

    fn headroom(&self) -> usize;
    fn tailroom(&self) -> usize;
    fn layout(&self) -> &BufferLayout;

    fn segments(&self) -> Self::Segments<'_>;
    fn first_segment(&self) -> Option<Segment<'_>>;
    fn contiguous(&self) -> Option<&[u8]>;
    fn read_at_exact(
        &self,
        offset: usize,
        dst: &mut [u8],
    ) -> Result<(), BufferAccessError>;
}

pub trait PacketBufferMut: PacketBuffer {
    type Frozen: PacketBuffer;

    type SegmentsMut<'a>: Iterator<Item = SegmentMut<'a>>;

    fn segments_mut(&mut self) -> Self::SegmentsMut<'_>;
    fn first_segment_mut(&mut self) -> Option<SegmentMut<'_>>;
    fn contiguous_mut(&mut self) -> Option<&mut [u8]>;

    fn prepend(&mut self, bytes: &[u8]) -> Result<(), ReserveError>;
    fn prepend_relocating(&mut self, bytes: &[u8]) -> Result<(), ReserveError>;
    fn extend_from_slice(&mut self, bytes: &[u8]) -> Result<(), BufferAccessError>;
    fn extend_from_slice_relocating(
        &mut self,
        bytes: &[u8],
    ) -> Result<(), BufferAccessError>;

    fn trim_prefix(&mut self, len: usize) -> Result<(), BufferAccessError>;
    fn trim_suffix(&mut self, len: usize) -> Result<(), BufferAccessError>;
    fn freeze(self) -> Self::Frozen;
}
```

`segments` exposes borrowed packet chunks in packet-byte order. Parsers that can
consume scatter-gather input can use it directly. Parsers that need one slice
can first try `contiguous`; when it returns `None`, the packet spans more than
one segment and the parser must either handle segments or copy the needed bytes
with `read_at_exact`. `first_segment` is useful for header parsers that only
need the leading contiguous bytes. Mutable buffers expose the same model through
`segments_mut`, `first_segment_mut`, and `contiguous_mut` for in-place parsing
and edits.

Some layers accept immutable packet buffers and then need to recover mutable
ownership to prepend or append protocol headers. Immutable buffers that support
that operation implement `OwnedPacketBuffer`:

```rust,ignore
pub trait OwnedPacketBuffer: PacketBuffer + Sized {
    type Mutable: PacketBufferMut<Frozen = Self>;

    fn into_mut(self) -> Self::Mutable;
}
```

The socket traits require their receive buffers, transmit-construction buffers,
and frozen transmit buffers to be `Send`. That lets applications move owned
packet buffers to other threads without copying packet bytes. The socket itself
does not need to be `Send`; concrete backends commonly keep sockets on their
owner thread.

Cross-thread buffers make reclamation more subtle. If a buffer is dropped on a
different thread, its drop path still has to return storage to the socket-owned
pool. Backends optimize this path carefully because the broader design tries to
avoid cross-core communication in the hot loop.

## Buffer Layouts and Socket Pools

Each socket maintains backend-owned buffer pools. Kernel bypass sockets need to
maintain packet memory that is directly accessible by the NIC through DMA.

Those pools hand out packet buffers. Applications do not borrow the pools
directly: receive buffers arrive through `recv`, and transmit buffers
are allocated through socket-level `allocate_tx_batch` methods. That lets each
backend batch allocation, drain completions, or use private pool-specific fast
paths without exposing pool types in the core socket traits.

Buffers handed out by socket pools have a layout, which is configurable through
the `BufferLayout` type:

```rust,ignore
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BufferLayout {
    payload_capacity: usize,
    headroom: usize,
    tailroom: usize,
    chunk_size: usize,
    data_offset: usize,
    align: NonZeroUsize,
    stride: usize,
    max_segments: usize,
    l2_headroom: usize,
    chunk_fixed: bool,
}
```

The defaults configured by socket factory methods are typically fine. A router
that forwards packets into an IP-over-IP tunnel may need extra headroom, while a
jumbo-frame application may need larger payload capacity.

An important performance optimization made by the library is that **no packet
buffer associated with a socket may outlive the socket**. This avoids including
an `Arc` pointing to the socket's internal pool in every buffer. Incrementing
the `Arc` on every packet adds significant overhead due to cross-core
communication.

## UDP Sockets

`UdpSocket` is the core trait for UDP payload sockets. Its associated types
select the receive buffer, mutable transmit buffer, polling driver, receive
metadata, and prepared endpoint handle.

For normal OS backends, a concrete `UdpSocket` maps to one OS socket. For
kernel-bypass backends, it usually maps to one NIC queue.

```rust,ignore
pub trait UdpSocket {
    type RxBuffer: PacketBufferMut + Send;
    type TxBufferMut: PacketBufferMut + Send;
    type Driver: PollDriver;
    type RecvMeta;
    type Endpoint;

    fn socket_id(&self) -> SocketId;
    fn mtu(&self) -> usize;
    fn worker_affinity(&self) -> QueueAffinity;
    fn capabilities(&self) -> UdpCapabilities;
    fn allocate_tx_batch(
        &mut self,
        out: &mut Vec<UdpTxBufferMut<Self>>,
        max: usize,
    ) -> Result<usize, Error>;
    fn driver(&self) -> &Self::Driver;
    fn driver_mut(&mut self) -> &mut Self::Driver;
    fn send(
        &mut self,
        batch: &mut [TxSlot<UdpTransmit<UdpTxBuffer<Self>>>],
    ) -> Result<usize, SendError>;
    fn prepare_udp_endpoint(
        &mut self,
        spec: UdpEndpointSpec,
    ) -> Result<Self::Endpoint, Error>;
    fn udp_endpoint_spec<'a>(&self, endpoint: &'a Self::Endpoint) -> &'a UdpEndpointSpec;
    fn udp_endpoint_info(&self, endpoint: &Self::Endpoint) -> UdpEndpointInfo;
    fn send_to_udp_endpoint(
        &mut self,
        endpoint: &mut Self::Endpoint,
        batch: &mut [TxSlot<UdpEndpointTransmit<UdpTxBuffer<Self>>>],
    ) -> Result<usize, SendError>;
    fn udp_endpoint_batch<'a>(
        &'a mut self,
        endpoint: &'a mut Self::Endpoint,
        max: usize,
    ) -> UdpEndpointBatchBuilder<'a, Self>;
    fn send_udp_endpoint_batch(
        &mut self,
        endpoint: &mut Self::Endpoint,
        max: usize,
        fill_payload: impl FnMut(usize, &mut [u8]) -> usize,
    ) -> Result<usize, SendError>;
    fn send_all(
        &mut self,
        batch: &mut [TxSlot<UdpTransmit<UdpTxBuffer<Self>>>],
    ) -> Result<usize, SendError>;
    fn send_all_to_udp_endpoint(
        &mut self,
        endpoint: &mut Self::Endpoint,
        batch: &mut [TxSlot<UdpEndpointTransmit<UdpTxBuffer<Self>>>],
    ) -> Result<usize, SendError>;
    fn recv(
        &mut self,
        out: &mut RecvBatch<UdpReceive<UdpRxBuffer<Self>, Self::RecvMeta>>,
    ) -> Result<usize, Error>;
    fn drain_tx_completions(&mut self) -> Result<usize, Error>;
    fn notify_tx(&mut self) -> Result<(), Error>;
}
```

A `UdpSocket` is not required to be `Send`. Concrete sockets are usually created
on the thread that drives them, which fits the design where one worker owns a
socket or a set of NIC queues.

Backend factory methods handle backend-specific setup such as mapping NIC queues
to workers, choosing NUMA-local memory, and applying thread-affinity hints.

Some methods accept `Vec`s as output storage. Callers are expected to reuse those
vectors and clear them between calls so the packet loop does not allocate.

Prepared UDP endpoints let applications move fixed destination and metadata
selection out of the per-packet transmit item. `UdpEndpointSpec` names the
remote peer plus optional source IP, source port, ECN, GSO segment size, and
fixed payload length. `prepare_udp_endpoint` returns a socket-specific
`Endpoint`, and `send_to_udp_endpoint` submits `UdpEndpointTransmit` slots that
only carry payload buffers. `udp_endpoint_batch` exposes a payload-generation
builder for callers that want to fill endpoint payloads directly. Backends that
do not have a specialized endpoint fast path can use `GenericUdpEndpoint`,
`prepare_generic_udp_endpoint`, `send_generic_udp_endpoint`, and
`send_generic_udp_endpoint_batch` to delegate through the normal socket paths
while preserving prefix ownership semantics.

The XDP UDP backend uses a specialized endpoint handle. It caches one
contiguous L2+IPv4+UDP header template, copies that header into packet headroom
in one operation, and patches only length-dependent fields plus the IPv4 header
checksum for variable-length endpoints. Queue-local route updates advance a
route generation; the next endpoint send clears any cached header from an older
generation and rebuilds it before accepting packets.

## IP Packet Sockets

`IpPacketSocket` follows the same buffer, batch, and polling model as
`UdpSocket`, but it moves complete IP datagrams instead of UDP payloads. This is
useful for lower-level forwarding paths and kernel-bypass backends that want to
generate or consume raw IP packets.

It adds two associated types:

- `Family`, an `IpFamily` policy selecting mixed IPv4/IPv6, IPv4-only, or
  IPv6-only address hints;
- `Egress`, the backend's resolved output handle for transmitted packets.

```rust,ignore
pub trait IpPacketSocket {
    type RxBuffer: PacketBufferMut + Send;
    type TxBufferMut: PacketBufferMut + Send;
    type Family: IpFamily;
    type Egress: IpPacketEgress;
    type Driver: PollDriver;
    type RecvMeta;

    fn socket_id(&self) -> SocketId;
    fn mtu(&self) -> usize;
    fn worker_affinity(&self) -> QueueAffinity;
    fn driver(&self) -> &Self::Driver;
    fn driver_mut(&mut self) -> &mut Self::Driver;
    fn allocate_tx_batch(
        &mut self,
        out: &mut Vec<IpPacketTxBufferMut<Self>>,
        max: usize,
    ) -> Result<usize, Error>;
    fn send(&mut self, batch: &mut [TxSlot<IpPacketTxItem<Self>>])
        -> Result<usize, SendError>;
    fn send_all(&mut self, batch: &mut [TxSlot<IpPacketTxItem<Self>>])
        -> Result<usize, SendError>;
    fn recv(&mut self, out: &mut RecvBatch<IpPacketRxItem<Self>>)
        -> Result<usize, Error>;
    fn drain_tx_completions(&mut self) -> Result<usize, Error>;
    fn notify_tx(&mut self) -> Result<(), Error>;
}
```

`IpPacketReceive` carries a complete IP datagram plus receive metadata such as
IP version, datagram length, and checksum status. `IpPacketTransmit` carries a
complete datagram, an egress handle, optional source and destination hints,
optional hop-limit handling, checksum offload flags, and an optional TSO segment
size.

The current core API has UDP and IP-packet socket traits. It does not expose a
link-level socket trait. Endpoint fast paths do not currently exist for
`IpPacketSocket`, but can be added later.

## Polling

Every socket has a `PollDriver`. The driver declares a compile-time `MODE`:
`PollMode::WaitDriven` or `PollMode::BusyPoll`.

```rust,ignore
pub trait PollDriver {
    const MODE: PollMode;

    fn wait(&mut self, timeout: Option<Duration>) -> Result<WaitOutcome, Error>;
    fn wake_handle(&self) -> Option<WakeHandle<'_>>;
}
```

Wait-driven drivers can wait for work through `wait(timeout)` and may expose a
borrowed `WakeHandle`. On Unix, that wake handle wraps a borrowed file
descriptor, which lets a caller integrate the socket into a poll loop.

Busy-poll drivers are for workers that repeatedly probe the socket. The core
`BusyPollDriver` does not sleep; its `wait` method returns a spurious outcome.

The marker traits `WaitDrivenDriverKind` and `BusyPollDriverKind` let generic
code select wait-driven or busy-poll socket loops at compile time.

## Routing and Egress

`IpFamily` is the type-level address-family policy used by routing and IP packet
metadata. The built-in policies are `Mixed`, `V4Only`, and `V6Only`.

The core API does not expose generic route-table, neighbor-table, or egress
resolver traits. Routing hooks are backend-specific because each backend needs
different context and returns different cached data-plane state. For example,
the XDP backend exposes `XdpUdpRouter`, whose resolved value can borrow cached
link-layer headers from queue-local route state.

## Device-Side API

`RawDevice` is optional. Raw backends implement it when applications need
device-side facts that do not belong in every packet operation:

- operating-system interface index;
- NIC queue ids;
- queue affinity hints;
- NUMA placement;
- raw-device capabilities such as checksum offload, RSS, TSO, GRO, and
  timestamping;
- per-queue and total statistics;
- MTU refresh after administrative changes.

High-level packet code can stay generic over `UdpSocket` or `IpPacketSocket`.
Operational code that needs queue or hardware facts can add a `RawDevice` bound.
