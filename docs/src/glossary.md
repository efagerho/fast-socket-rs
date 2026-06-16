# Glossary

This chapter covers the public traits and structs exported by the
`fast-socket-rs` core crate. Enums and type aliases appear in other chapters or
in rustdoc when they are needed for context; this glossary focuses on the API
types applications and backends usually implement or carry.

The definitions below are signature-only: default method bodies are omitted so
each trait's shape is easy to scan.

## Traits

### `PacketBuffer`

`PacketBuffer` is the immutable view of owned packet bytes. It exposes the
packet length, headroom, tailroom, allocation layout, segment iterator, and safe
read operations.

It exists so packet-processing code can inspect packet data without caring
whether the bytes live in a heap buffer, a DMA chunk, a single contiguous slice,
or a scatter-gather layout.

```rust,ignore
pub trait PacketBuffer {
    type Segments<'a>: Iterator<Item = Segment<'a>>
    where
        Self: 'a;

    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn headroom(&self) -> usize;
    fn tailroom(&self) -> usize;
    fn layout(&self) -> &BufferLayout;
    fn segments(&self) -> Self::Segments<'_>;
    fn read_at_exact(
        &self,
        offset: usize,
        dst: &mut [u8],
    ) -> Result<(), BufferAccessError>;
}
```

### `PacketBufferMut`

`PacketBufferMut` extends `PacketBuffer` with packet construction and editing
operations. It can prepend bytes, append bytes, trim either end of the packet,
and freeze the mutable buffer into an immutable transmit-ready buffer.

It exists to make packet mutation generic over storage backends. Application
code can build headers and payloads in place while the concrete buffer type
decides how that maps onto its memory layout.

```rust,ignore
pub trait PacketBufferMut: PacketBuffer {
    type Frozen: PacketBuffer;

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

### `OwnedPacketBuffer`

`OwnedPacketBuffer` is implemented by immutable packet buffers that can be
converted back into their mutable form.

It exists for layering. A higher layer may receive or store an owned frozen
payload, then later need to recover mutable ownership so it can prepend or
append lower-layer headers before transmit.

```rust,ignore
pub trait OwnedPacketBuffer: PacketBuffer + Sized {
    type Mutable: PacketBufferMut<Frozen = Self>;

    fn into_mut(self) -> Self::Mutable;
}
```

### `BufferPool`

`BufferPool` allocates mutable packet buffers with a shared `BufferLayout`. It
returns `None` when no buffer is immediately available.

It exists to keep packet allocation and reuse out of the hot-path socket API.
Backends can provide fixed-size, pre-registered, or recycled memory while users
interact with a single allocation interface.

```rust,ignore
pub trait BufferPool {
    type Buffer: PacketBufferMut;

    fn layout(&self) -> &BufferLayout;
    fn allocate(&mut self) -> Option<Self::Buffer>;
}
```

### `RawDevice`

`RawDevice` exposes optional device-side information for raw backends:
interface identity, NIC queue ids, queue affinity, NUMA placement, capabilities,
statistics, and MTU refresh.

It exists so code that needs hardware or queue facts can get them without
putting device-management concerns directly into `UdpSocket` or
`IpPacketSocket`.

```rust,ignore
pub trait RawDevice {
    fn ifindex(&self) -> IfIndex;
    fn nic_queues(&self) -> &[QueueId];
    fn capabilities(&self) -> Capabilities;
    fn queue_affinity(&self, queue: QueueId) -> QueueAffinity;
    fn queue_numa_node(&self, queue: QueueId) -> Option<NumaNode>;
    fn stats(&self, queue: QueueId) -> RawDeviceStats;
    fn total_stats(&self) -> RawDeviceStats;
    fn refresh_mtu(&mut self) -> Result<u32, Error>;
}
```

### `PollDriver`

`PollDriver` is the companion polling interface attached to sockets. It declares
the compile-time `PollMode`, waits for work or timeout, and optionally exposes a
borrowed wake handle.

It exists to separate "how this socket waits" from "how this socket moves
packets." The `MODE` constant is the only polling-mode classifier; applications
can branch on it once at startup and then run the matching worker loop directly.

```rust,ignore
pub trait PollDriver {
    const MODE: PollMode;

    fn wait(&mut self, timeout: Option<Duration>) -> Result<WaitOutcome, Error>;
    fn wake_handle(&self) -> Option<WakeHandle<'_>>;
}
```

### `IpFamily`

`IpFamily` is a type-level IP-family policy. Its associated `Addr` type is the
address representation used by routing and IP packet transmit metadata.

It exists to let code specialize for mixed IPv4/IPv6 sockets, IPv4-only sockets,
or IPv6-only sockets without passing a runtime family flag through every packet
operation.

```rust,ignore
pub trait IpFamily {
    type Addr: Copy + Eq;
}
```

### `IpPacketEgress`

`IpPacketEgress` is the marker trait for egress handles accepted by
`IpPacketSocket` transmit operations.

It exists so each backend can choose the egress representation that matches its
routing model while the core IP packet trait remains generic over that handle.

```rust,ignore
pub trait IpPacketEgress: Copy + 'static {}
```

### `RouteTable`

`RouteTable` resolves a destination IP address into a `RouteHop`: the outgoing
interface and next-hop address.

It exists to keep layer-3 route lookup separate from sockets and transmit
descriptors. Applications can use a general route table or provide a specialized
one that encodes deployment-specific knowledge.

```rust,ignore
pub trait RouteTable<F: IpFamily = Mixed> {
    fn resolve_route(&self, dst: F::Addr) -> Option<RouteHop<F::Addr>>;
}
```

### `NeighborTable`

`NeighborTable` resolves a next-hop IP address into a link-layer address.

It exists to keep layer-2 neighbor resolution separate from route lookup. That
separation lets an application replace only the neighbor path, cache fixed
answers, or compose different route and neighbor implementations.

```rust,ignore
pub trait NeighborTable<F: IpFamily = Mixed> {
    fn resolve_l2(&self, next_hop: F::Addr) -> Option<LinkAddr>;
}
```

### `EgressResolver`

`EgressResolver` resolves a destination IP address directly into the concrete
egress handle consumed by an IP packet socket.

It exists as the composition point between routing, neighbor resolution, and
backend-specific transmit handles. General code can resolve egress through route
and neighbor tables, while specialized code can return a precomputed handle.

```rust,ignore
pub trait EgressResolver<F: IpFamily, E: IpPacketEgress> {
    fn resolve_egress(&self, dst: F::Addr) -> Option<E>;
}
```

### `UdpSocket`

`UdpSocket` is the high-level socket trait for UDP payloads. It owns receive and
transmit pools, exposes capabilities and a polling driver, receives
`UdpReceive` batches, sends `UdpTransmit` slots, allocates transmit buffers, and
drains transmit completions.

It exists to give applications one UDP packet API across operating-system
sockets, kernel-bypass backends, and test implementations while preserving the
core ownership model.

```rust,ignore
pub trait UdpSocket
where
    <Self::RxPool as BufferPool>::Buffer: Send,
    <<Self::RxPool as BufferPool>::Buffer as PacketBufferMut>::Frozen: Send,
    <Self::TxPool as BufferPool>::Buffer: Send,
    <<Self::TxPool as BufferPool>::Buffer as PacketBufferMut>::Frozen: Send,
{
    type RxPool: BufferPool;
    type TxPool: BufferPool;
    type Driver: PollDriver;
    type RecvMeta;

    fn socket_id(&self) -> SocketId;
    fn mtu(&self) -> usize;
    fn worker_affinity(&self) -> QueueAffinity;
    fn capabilities(&self) -> UdpCapabilities;
    fn rx_pool(&self) -> &Self::RxPool;
    fn rx_pool_mut(&mut self) -> &mut Self::RxPool;
    fn tx_pool(&self) -> &Self::TxPool;
    fn tx_pool_mut(&mut self) -> &mut Self::TxPool;
    fn allocate_tx_batch(
        &mut self,
        out: &mut Vec<UdpTxBufferMut<Self>>,
        max: usize,
    ) -> Result<usize, Error>
    where
        Self: Sized;
    fn driver(&self) -> &Self::Driver;
    fn driver_mut(&mut self) -> &mut Self::Driver;
    fn send(
        &mut self,
        batch: &mut [TxSlot<UdpTransmit<UdpTxBuffer<Self>>>],
    ) -> Result<usize, SendError>;
    fn send_all(
        &mut self,
        batch: &mut [TxSlot<UdpTransmit<UdpTxBuffer<Self>>>],
    ) -> Result<usize, SendError>
    where
        Self: Sized;
    fn recv(
        &mut self,
        out: &mut RecvBatch<UdpReceive<UdpRxBuffer<Self>, Self::RecvMeta>>,
    ) -> Result<usize, Error>;
    fn drain_tx_completions(&mut self) -> Result<usize, Error>;
    fn notify_tx(&mut self) -> Result<(), Error>;
}
```

### `IpPacketSocket`

`IpPacketSocket` is the socket trait for complete IP datagrams. It owns receive
and transmit pools, exposes a polling driver, receives `IpPacketReceive` batches,
sends `IpPacketTransmit` slots, and drains transmit completions.

It exists for applications and backends that want to work below UDP while still
using the same packet ownership, batch, and polling model.

```rust,ignore
pub trait IpPacketSocket
where
    <Self::RxPool as BufferPool>::Buffer: Send,
    <<Self::RxPool as BufferPool>::Buffer as PacketBufferMut>::Frozen: Send,
    <Self::TxPool as BufferPool>::Buffer: Send,
    <<Self::TxPool as BufferPool>::Buffer as PacketBufferMut>::Frozen: Send,
{
    type RxPool: BufferPool;
    type TxPool: BufferPool;
    type Family: IpFamily;
    type Egress: IpPacketEgress;
    type Driver: PollDriver;
    type RecvMeta;

    fn socket_id(&self) -> SocketId;
    fn mtu(&self) -> usize;
    fn worker_affinity(&self) -> QueueAffinity;
    fn rx_pool(&self) -> &Self::RxPool;
    fn rx_pool_mut(&mut self) -> &mut Self::RxPool;
    fn tx_pool(&self) -> &Self::TxPool;
    fn tx_pool_mut(&mut self) -> &mut Self::TxPool;
    fn driver(&self) -> &Self::Driver;
    fn driver_mut(&mut self) -> &mut Self::Driver;
    fn send(&mut self, batch: &mut [TxSlot<IpPacketTxItem<Self>>])
        -> Result<usize, SendError>;
    fn recv(&mut self, out: &mut RecvBatch<IpPacketRxItem<Self>>)
        -> Result<usize, Error>;
    fn drain_tx_completions(&mut self) -> Result<usize, Error>;
    fn notify_tx(&mut self) -> Result<(), Error>;
}
```

## Structs

### Batch structs

- `RecvBatch<T>` is reusable caller-owned receive storage with a fixed item
  capacity. It lets receive loops avoid allocating a new batch on every
  iteration.
- `SendError` reports a failed batch send after a prefix was accepted. Its
  `accepted` field tells the caller which leading `TxSlot` values have already
  been consumed.

### Buffer structs

- `ScatterGather<'a>` is a borrowed view of packet segments in packet-byte
  order.
- `BufferLayout` describes the memory facts for packet chunks: payload
  capacity, headroom, tailroom, alignment, stride, L2 headroom, and segment
  count.
- `QueueBufferConfig` groups receive and transmit `BufferLayout` values with
  optional queue depths.
- `BufferCapabilities` records static or queue-local buffer limits, including
  packet length, headroom, tailroom, segment count, DMA capability, and external
  registration.

### Device structs

- `Capabilities` is the raw-device capability bitset for features such as
  checksum offload, RSS, TSO, GRO, and timestamping.
- `RawDeviceStats` is a snapshot of cumulative raw device or queue counters.

### Error structs

- `DeviceError` stores a coarse `DeviceErrorKind` and an optional shared source
  error. It lets the core `Error` type remain cloneable without losing backend
  error context.

### IP packet structs

- `CoreEgress` is the default core egress enum for implementations that do not
  need a custom egress handle.
- `IpPacketRecvMeta` is the default receive metadata for complete IP datagrams:
  IP version, datagram length, and checksum status.
- `TxOffload` is the transmit offload bitset for IP checksum and L4 checksum
  requests.
- `IpPacketReceive<B, M>` pairs a received IP datagram buffer with receive
  metadata.
- `IpPacketTransmit<B, E, F>` carries a complete IP datagram, resolved egress
  handle, optional address hints, hop-limit hint, checksum offload flags, and
  optional TSO segment size.

### Policy structs

- `WakeHandle<'a>` is a borrowed wake handle. On Unix it wraps a borrowed
  file descriptor; on non-Unix targets it wraps an opaque token.
- `BusyPollDriver` is the standard busy-poll `PollDriver`. Its `wait` method
  does not sleep and returns a spurious outcome.
- `Mixed` is the IP-family policy for sockets and tables that handle both IPv4
  and IPv6 addresses.
- `V4Only` is the IP-family policy for IPv4-only sockets and tables.
- `V6Only` is the IP-family policy for IPv6-only sockets and tables.

### Route structs

- `RouteId` is an opaque route identifier used by core egress handles and route
  table implementations.
- `NeighborId` is an opaque neighbor identifier used by core egress handles and
  neighbor table implementations.
- `LinkAddr` is a six-octet link-layer address.
- `RouteHop<A>` is the result of route lookup: outgoing interface plus next-hop
  address.

### System structs

- `HugePageSize` describes a hugepage preference for backends that allocate
  packet memory from huge pages.
- `IfIndex` is a non-zero operating-system interface index.
- `QueueAffinity` describes the CPU affinity hint for a queue or socket worker.
- `QueueId` identifies a queue within a port, interface, or device.
- `SocketId` identifies a logical socket separately from any backing queue id.
- `NumaNode` identifies a NUMA node.

### UDP structs

- `UdpRecvMeta` is the default UDP receive metadata: source address, optional
  destination IP, optional destination port, optional ECN codepoint, payload
  length, and optional GRO stride.
- `UdpReceive<B, M>` pairs a received UDP payload buffer with receive metadata.
- `UdpTransmit<B>` carries a UDP payload buffer plus destination address,
  optional source IP, optional source port, optional ECN codepoint, and optional
  GSO segment size.
- `UdpCapabilities` records UDP-specific socket features such as GSO, GRO, and
  maximum GSO segment count.
