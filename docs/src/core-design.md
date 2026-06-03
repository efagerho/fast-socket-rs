# Core Design

`fast-socket-rs` is designed around one main idea: the packet fast path should do
as little work as possible. The library should make it practical to build network
programs where receiving, preparing, and submitting packets can stay close to the
hardware without paying for unnecessary copies, allocations, or runtime
indirection.

The default implementation should be fast enough for general use, but the design
also leaves room for applications that know more than a general-purpose stack can
know. When a program can prove stronger assumptions about its traffic, peers, or
deployment environment, it should be able to move that knowledge into the type
system or compile-time configuration and remove work from the hot path.

## No Copies on Kernel Bypass Backends

For kernel bypass backends, packet buffers are treated as the unit of ownership.
The goal is to pass those buffers through the receive, processing, and transmit
paths without copying packet contents.

This matters because memory bandwidth is often the real bottleneck in high packet
rate systems. A copy that looks small in isolation becomes expensive when it is
performed tens of millions of times per second. The library therefore prefers
APIs that let packet data stay in backend-owned or pool-owned buffers while the
application mutates metadata and headers in place.

The design assumes sockets outlive any buffers handed out by their pools. That
lets the API keep buffer movement cheap without adding defensive ownership checks
to every operation in the fast path.

## No Heap Allocations for Packet Processing

Packet processing should not require heap allocation. Allocation adds latency,
creates allocator contention, and makes performance harder to reason about under
load.

Instead, the design favors preallocated packet pools, fixed-capacity structures,
and caller-provided storage. The hot path should be able to operate on buffers,
descriptors, and stack-local state that already exist before packets arrive.

This does not mean the whole library can never allocate. Setup code, control
planes, and convenience APIs may allocate where that is appropriate. The design
line is the packet processing path: once packets are flowing, the common receive
and transmit operations should not need to touch the heap.

## Compile-Time Specialization

Runtime checks are useful at the edges of a system, but they are expensive when
they sit inside the loop that handles every packet. `fast-socket-rs` therefore
prefers marker traits and type-level capabilities when a decision can be made at
compile time.

Marker traits let the library describe what a backend, socket, buffer, route
provider, or neighbor provider can do. Generic code can then be monomorphized for
the concrete combination used by the application. The compiler sees the exact
types involved and can inline, eliminate dead branches, and produce code that is
closer to a hand-written fast path for that configuration.

This approach keeps the default APIs expressive while avoiding a design where
every transmit or receive operation repeatedly asks questions that were already
answered when the program was compiled.

## Fast Tx Submission

Submitting a transmit buffer to the NIC should take as few clock cycles as
possible. By the time a packet reaches the final submission step, the library
should already know the buffer layout, the backend-specific descriptor format,
and the minimum metadata needed by the NIC.

The Tx path is therefore designed to avoid late work. It should not copy packet
contents, allocate temporary state, perform avoidable dispatch, or recompute
headers that the application could have prepared earlier. The ideal submission
path is a small amount of pointer, length, and descriptor bookkeeping followed by
the backend's enqueue operation.

The library's abstractions are judged by this path. If an abstraction makes Tx
submission clearer but costs extra work per packet, it needs to justify that cost
or provide a way to specialize it away.

## Escape Hatches for Maximum Performance

The default configuration should be broadly useful. It should handle ordinary
routing and neighbor discovery without asking every application to become its own
network stack. Today, the default routing table can already push past 20 million
packets per second on a single core.

Some programs, however, know enough about their environment to do even less work.
For those programs, the library should expose compile-time and type-level escape
hatches that replace general mechanisms with specialized ones.

For example, an application may only talk to a single peer. Another application
may know that it only sends IP packets on the local subnet to the default
gateway. In both cases, the program can cache a single L2 header and reuse it
for outgoing packets.

That optimization can be expressed by overriding the default routing table and
neighbor discovery mechanism. Instead of resolving routes and neighbors through
the general path, the application supplies specialized implementations whose
answer is already known. Once those implementations are part of the concrete
socket type, monomorphization lets the compiler remove the unused generality from
the hot path.

These escape hatches are not meant to make the common case harder. They are a way
to keep the general case ergonomic while still giving performance-critical
applications a path to encode their deployment assumptions directly into the
generated code.
