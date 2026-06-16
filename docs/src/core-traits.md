# Core Traits

The core traits describe packet memory, socket-owned pools, batch ownership,
polling, routing, and the socket APIs that move packets between an application
and a backend.

This chapter is a map of the current public surface. Backend-specific builders
and tile APIs are covered in later chapters.

## Packet Buffers

`PacketBuffer` is the immutable view of owned packet bytes. It exposes packet
length, public headroom and tailroom, the `BufferLayout`, packet segments, and a
safe `read_at_exact` method that works across segment boundaries.

`PacketBufferMut` extends `PacketBuffer` with packet editing operations:

- `prepend` writes bytes before the current packet start;
- `prepend_relocating` may move existing packet bytes when the backend supports
  relocation;
- `extend_from_slice` appends bytes after the current packet end;
- `extend_from_slice_relocating` may relocate when appending;
- `trim_prefix` and `trim_suffix` remove bytes from either end;
- `freeze` turns the mutable buffer into its immutable transmit form.

The direct append and prepend methods fail if the requested operation does not
fit the current public headroom or tailroom. The relocating variants default to
the direct methods, so code can call the relocation form without requiring every
backend to implement movement.

`OwnedPacketBuffer` is implemented by immutable packet buffers that can be
converted back into their mutable form. That is useful for layering: one API can
own a frozen payload and a lower layer can later recover mutable ownership to add
headers.

## Layouts and Pools

`BufferLayout` describes one packet allocation shape. It records payload
capacity, public headroom, public tailroom, link-layer headroom, total chunk
size, data offset, alignment, stride, and maximum segment count.

Common constructors are:

- `BufferLayout::new`, for a contiguous payload-capacity layout;
- `BufferLayout::for_payload`, which rounds small payload capacities up to a
  packet-friendly floor;
- `BufferLayout::with_headroom_and_tailroom`, for protocols that need space
  before or after the initial packet bytes;
- `with_l2_headroom`, `with_alignment`, `with_max_segments`, and
  `with_fixed_chunk`, for backend memory facts such as AF_XDP frame layout.

`QueueBufferConfig` groups receive and transmit layouts with optional queue
depths. `BufferCapabilities` records static buffer limits such as maximum packet
length, maximum headroom, maximum tailroom, scatter-gather segment count, DMA
capability, and external registration.

`BufferPool` allocates mutable packet buffers with one shared layout. It returns
`None` when no buffer is immediately available. Sockets own their receive and
transmit pools; applications borrow those pools through socket methods or use
helpers such as `UdpSocket::allocate_tx_batch`.

The pool object itself does not need to be `Send`. The buffers handed out by
socket pools do: `UdpSocket` and `IpPacketSocket` require mutable RX/TX buffers
and their frozen forms to be `Send`. That lets an owned packet buffer move to
another thread, while the live socket remains driven by its owner.

The lifetime rule is external to the type system: a socket and its pools must
outlive every buffer they hand out.

## Batch Ownership

`RecvBatch<T>` is caller-provided receive storage with fixed item capacity. A
worker usually creates one batch per loop, calls `clear` before receiving, and
drains the items after `recv`. `RecvBatch::with_capacity` requires capacity at
least one.

`TxSlot<T>` is the transmit ownership container. `TxSlot::Ready` contains a
packet still owned by the caller. When a socket accepts a packet, it takes the
packet out of the slot and leaves `TxSlot::Taken`.

`send` accepts packets in order. On success, the returned count is the accepted
prefix. On failure, `SendError::accepted` reports how many leading slots were
accepted before the failing slot. Slots after the accepted prefix remain in the
caller-provided slice and still belong to the caller.

## Polling

Every socket has a `PollDriver`. The driver declares a compile-time `MODE`:
`PollMode::WaitDriven` or `PollMode::BusyPoll`.

Wait-driven drivers can wait for work through `wait(timeout)` and may expose a
borrowed `WakeHandle`. On Unix, that wake handle wraps a borrowed file
descriptor, which lets a caller integrate the socket into a poll loop.

Busy-poll drivers are for workers that repeatedly probe the socket. The core
`BusyPollDriver` does not sleep; its `wait` method returns a spurious outcome.

The marker traits `WaitDrivenDriverKind` and `BusyPollDriverKind` are used by
the tile runtime to pair parked tiles with wait-driven sockets and spinning
tiles with busy-poll sockets.

## UDP Sockets

`UdpSocket` is the core trait for UDP payload sockets. Its associated types
select the receive pool, transmit pool, polling driver, and receive metadata.

The identity and configuration methods are:

- `socket_id`, a logical socket id;
- `mtu`, the maximum UDP payload length accepted for transmit;
- `worker_affinity`, a CPU or queue-affinity hint;
- `capabilities`, which reports UDP GSO/GRO support and maximum GSO segment
  count when known.

The pool and driver methods expose the socket-owned RX pool, TX pool, and poll
driver. `allocate_tx_batch` allocates mutable TX buffers and, when the pool is
empty, drains transmit completions once before retrying.

The packet methods are:

- `recv`, which fills a `RecvBatch<UdpReceive<...>>`;
- `send`, which consumes an accepted prefix of `TxSlot<UdpTransmit<_>>`;
- `send_all`, which loops `send` and drains completions until all slots are
  accepted or an error occurs;
- `drain_tx_completions`, which reclaims completed transmit buffers;
- `notify_tx`, which lets backends flush or kick transmission when needed.

`UdpRecvMeta` carries the source address, optional local destination IP,
optional local destination port, optional ECN codepoint, payload length, and
optional GRO stride.

`UdpTransmit` carries a frozen payload buffer plus destination address, optional
source IP, optional source port, optional ECN codepoint, and optional UDP GSO
segment size.

## IP Packet Sockets

`IpPacketSocket` follows the same pool, batch, and polling model as
`UdpSocket`, but it moves complete IP datagrams instead of UDP payloads.

It has two additional associated types:

- `Family`, an `IpFamily` policy selecting mixed IPv4/IPv6, IPv4-only, or
  IPv6-only address hints;
- `Egress`, the backend's resolved output handle for transmitted packets.

`recv` returns `IpPacketReceive` items containing complete IP datagrams.
`send` accepts `IpPacketTransmit` items wrapped in `TxSlot`, using the same
accepted-prefix and `SendError` rules as UDP sends. IP packet sockets also
expose `drain_tx_completions` and `notify_tx`.

Unlike `UdpSocket`, the current `IpPacketSocket` trait does not provide
`allocate_tx_batch` or `send_all` convenience methods.

`IpPacketRecvMeta` reports IP version, datagram length, and checksum status.
`IpPacketTransmit` carries the datagram, egress handle, optional source and
destination address hints, optional hop-limit hint, checksum offload flags, and
optional TSO segment size.

## Routing and Egress

`IpFamily` is the type-level address-family policy used by routing and IP packet
metadata. The built-in policies are `Mixed`, `V4Only`, and `V6Only`.

`RouteTable` maps a destination IP address to a `RouteHop`: outgoing interface
and next-hop address. `NeighborTable` maps a next-hop address to a link-layer
address. `EgressResolver` maps a destination directly to the concrete egress
handle consumed by a backend.

These traits are intentionally small. A general implementation can compose route
and neighbor tables. A specialized implementation can return a cached egress
handle or precomputed link-layer information.

## Device Side API

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
