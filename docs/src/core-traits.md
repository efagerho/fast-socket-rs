# Core Traits

The core traits describe the packet ownership model used by every backend. They
cover packet buffers, socket-owned buffer pools, batch ownership, and the socket
interfaces that move packets between an application and a backend.

This chapter stays on the hot-path abstractions. Metrics, device information,
capability discovery, and acceleration-specific options are separate topics.

## Packet Buffers

`PacketBuffer` is the read-only view of owned packet bytes. It reports packet
length, available headroom and tailroom, the allocation layout, and the packet's
segments. Code that only needs to inspect packet data should work with this
trait.

`PacketBufferMut` extends `PacketBuffer` with mutation operations:

- prepend bytes before the packet;
- append bytes after the packet;
- trim bytes from either side;
- freeze the mutable buffer into an immutable packet buffer.

Freezing is the handoff point used by transmit paths. Application code builds a
packet in a mutable buffer, freezes it, and submits the frozen buffer to the
socket.

`OwnedPacketBuffer` is for immutable buffers that can be converted back into
their mutable form. That is useful when a layer receives an owned payload and
must add headers before handing it to a lower layer.

## Buffer Pools

`BufferPool` allocates mutable packet buffers with a fixed `BufferLayout`. The
layout describes payload capacity, headroom, tailroom, alignment, and segment
shape. A pool returns `None` when no buffer is immediately available.

Socket traits accept ordinary `BufferPool` implementations. The pools
themselves are not required to be `Send`; they can remain worker-local. The
threading contract is on the buffers handed out by socket pools: mutable buffers
and their frozen form must both be `Send`, so an owned packet buffer can move to
another thread and be dropped there.

Sockets own their pools. `UdpSocket::RxPool` and `IpPacketSocket::RxPool` provide
storage for received packets. `UdpSocket::TxPool` and `IpPacketSocket::TxPool`
provide storage for packets the application wants to transmit.

The design assumes sockets outlive any buffers handed out by their pools. The
live socket object should still be driven by its owning worker thread unless a
backend documents stronger threading guarantees.

## Batch Ownership

`RecvBatch` is caller-provided receive storage with fixed capacity. A worker
usually creates one batch per loop, reuses it with `clear`, and drains received
items after each `recv` call.

`TxSlot` is the transmit ownership container. A `TxSlot::Ready` slot contains a
packet that still belongs to the caller. When a socket accepts a packet for
transmit, it takes the packet out of the slot and leaves `TxSlot::Taken` behind.

`send` accepts packets in order. On success, the returned count is the accepted
prefix. On failure, `SendError::accepted` reports how many leading slots were
accepted before the failing slot. Slots after that prefix still belong to the
caller.

## UDP Sockets

`UdpSocket` is the core trait for UDP payload sockets. It has associated receive
and transmit pools, a polling driver, and a receive metadata type.

The main packet operations are:

- `recv`, which fills a `RecvBatch` with `UdpReceive` items;
- `send`, which submits a slice of `TxSlot<UdpTransmit<_>>`;
- `send_all`, which keeps submitting until all slots are accepted or an error
  occurs;
- `drain_tx_completions`, which reclaims completed transmit buffers;
- `notify_tx`, which flushes backends that need an explicit transmit kick.

`allocate_tx_batch` is a convenience helper on `UdpSocket`. It allocates mutable
transmit buffers from `UdpSocket::TxPool` and drains completions once if the pool
is empty. Worker loops can use it to prepare packet storage without knowing how
the backend reclaims transmit buffers.

`UdpReceive` carries a received payload buffer plus metadata such as peer address
when the backend provides it. `UdpTransmit` carries a frozen payload buffer plus
the destination address.

## IP Packet Sockets

`IpPacketSocket` follows the same pool, batch, and polling model as `UdpSocket`,
but it moves complete IP datagrams instead of UDP payloads.

The trait adds two type-level choices:

- `IpPacketSocket::Family`, which describes whether the socket handles IPv4,
  IPv6, or both;
- `IpPacketSocket::Egress`, which is the backend's resolved output handle for a
  transmitted packet.

`recv` returns `IpPacketReceive` items containing complete IP datagrams. `send`
accepts `IpPacketTransmit` items wrapped in `TxSlot`, using the same accepted
prefix and `SendError` rules as `UdpSocket`.

## Polling Model

Every socket has a `PollDriver`. The driver records whether the socket is meant
to be used in wait-driven mode or busy-poll mode.

Wait-driven sockets can wait on an external event source. A worker tries to
receive, send, and drain completions first, then calls the driver when it has no
work left.

Busy-poll sockets are intended for workers that repeatedly probe the socket.
Their driver does not sleep; the worker decides whether to spin, run periodic
maintenance, or yield when no packets are available.

Applications can select the appropriate worker loop once at startup from
`<S::Driver as PollDriver>::MODE`. That keeps the mode branch outside the packet
hot path while avoiding separate wait-driven/busy-poll marker traits.
