# Core Design

`fast-socket-rs` is a small set of packet ownership and socket-driving
abstractions. Backend crates implement those abstractions for operating-system
UDP sockets, AF_XDP queues, and higher-level UDP tiles.

The common design goal is simple: keep the packet path explicit. Applications
should be able to see where packet memory comes from, who owns it, when a socket
is driven, and where back-pressure can occur.

## Packet Ownership

Packets are owned buffer values. A socket owns receive and transmit pools, and
those pools hand out buffers that can move through the application before they
are dropped or submitted back to a socket.

On receive, a backend fills a mutable receive buffer from the socket's receive
pool and returns it in a `RecvBatch`. On transmit, application code builds a
packet in a mutable transmit buffer, freezes it, wraps it in a transmit item,
and submits it through `TxSlot`.

The crate-wide lifetime rule is that sockets and their pools outlive every
buffer they hand out. This is part of the performance contract: backends can
make buffer movement cheap because they do not need to protect every packet
operation with defensive owner checks.

Live sockets are still meant to be driven by their owning worker thread unless a
backend documents stronger guarantees. Buffers are `Send`, so applications can
move owned packet buffers to other threads, but the socket object itself remains
the thing that owns polling, completion draining, and pool reuse.

## Reused Hot-Path Storage

The core APIs avoid requiring allocation in the packet loop. Receive uses
caller-provided `RecvBatch` storage with fixed item capacity. Transmit uses
caller-owned `Vec<TxSlot<_>>` storage so applications can retain unaccepted
packets and retry them according to their own policy.

Sockets own their packet pools. `BufferLayout` describes the memory facts for
those pools: payload capacity, public headroom and tailroom, link-layer
headroom, alignment, chunk size, stride, and maximum segment count. Operating
system sockets use heap-backed single-segment buffers; AF_XDP sockets use
layouts that describe UMEM frame constraints and L2 headroom.

The API does not promise that every backend is zero-copy. The OS backend still
copies across the kernel boundary because ordinary UDP sockets do. The shared
abstractions are shaped so that, after a backend has placed bytes in a packet
buffer, application code does not need extra packet-object copies to process or
forward them.

## Batch Submission

Transmit is prefix-based. A socket accepts slots in order, takes ownership of
each accepted packet by changing its `TxSlot` to `Taken`, and reports how many
leading slots were accepted. If an error happens after partial progress,
`SendError::accepted` tells the caller exactly which prefix is gone and which
tail still belongs to the caller.

This model fits both system-call backends and ring-based backends. A Linux OS
socket can translate a prefix into `sendmmsg` work. An AF_XDP socket can
translate a prefix into descriptors. The application sees the same ownership
rule either way.

Completions are explicit. Sockets expose `drain_tx_completions` because some
backends cannot reuse transmit buffers until hardware or the kernel reports
that transmission finished. `UdpSocket::allocate_tx_batch` is a convenience
helper that allocates transmit buffers and drains completions once when the pool
is empty, but applications that use the direct socket API still decide when that
work runs.

TODO: We should still optimize batch buffer allocation.

## Compile-Time Shape

The core traits use associated types for the pieces that define a socket's
packet path:

- receive and transmit pools;
- receive metadata;
- polling driver;
- IP family and egress handle for complete IP datagram sockets.

Those associated types let generic worker code be monomorphized for the exact
backend and packet representation in use. The same pattern appears in routing:
`RouteTable`, `NeighborTable`, and `EgressResolver` let general code use normal
routing state while specialized applications can provide static or precomputed
answers.

Polling is also type-shaped. Every socket has a `PollDriver` with a compile-time
`PollMode`. A worker can choose a wait-driven or busy-poll loop once at startup
instead of branching on the polling regime for every packet.

## Backends and Layers

The core crate defines traits; backend crates decide how to implement them.

`fast-socket-os-rs` implements `UdpSocket` on top of nonblocking OS UDP
sockets. It is wait-driven, portable across supported Unix platforms, and useful
when an application wants the same packet API without requiring kernel bypass.

`fast-socket-xdp-rs` implements AF_XDP-shaped `IpPacketSocket` and `UdpSocket`
types. It owns queue-local UMEM pools, raw rings, route snapshots, and XDP
egress handles. It can be opened in busy-poll or wait-driven form.

`fast-socket-udp-tile` is an application-facing layer over UDP sockets. A tile
worker owns one or more sockets and exchanges packets with application lanes
through bounded queues. Backend tile crates provide convenient OS and AF_XDP
builders.

This layering gives application writers a choice. They can work directly with a
socket trait and control the worker loop themselves, or they can use tiles and
let the tile runtime own socket polling, transmit buffering, lane routing, and
thread pinning.

## Specialization Points

The default path is intended to be usable without writing a network stack. At
the same time, the APIs expose places where deployment knowledge can remove work
from the packet path.

An application that only talks to a fixed peer can use a router or egress
resolver that returns a precomputed answer. An AF_XDP UDP router can cache and
borrow prebuilt L2 headers. A tile classifier can send flows to stable lanes so
application state stays local. A backend can expose `RawDevice` facts such as
queue affinity, NUMA placement, capabilities, and counters without forcing those
concerns into every packet operation.

These escape hatches are not separate fast and slow APIs. They are the same
traits with more specific associated types and implementations.
