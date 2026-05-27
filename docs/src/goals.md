# Goals and Non-Goals

Fast Socket makes high-rate packet I/O explicit without tying callers to one
backend. The public API stays consistent while each backend keeps its fast
representation.

## Goals

- Provide backend-agnostic `UdpSocket` and `IpPacketSocket` traits.
- Support OS sockets and kernel-bypass backends under the same core vocabulary.
- Make batch send, batch receive, and completion draining first-class.
- Expose buffer ownership and layout facts instead of hiding allocator behavior.
- Keep queue ownership clear enough for CPU affinity, NUMA locality, and
  single-threaded packet memory.
- Keep optional features, routes, tunnels, metadata, device APIs, and backend
  control paths outside unrelated hot loops.

## Zero-Overhead Rules

Zero overhead means unused abstractions must not add measurable cost to
steady-state packet paths.

- use static dispatch for steady-state packet operations;
- avoid trait objects in send, receive, completion, and polling loops unless a
  caller explicitly chooses them;
- keep optional features represented by concrete policy and metadata types;
- avoid packet-byte copies except at explicit relocation or backend copy
  boundaries;
- make batch operations preserve ownership without hidden allocation;
- keep completion draining explicit for zero-copy transmit paths;
- keep device control and statistics off the ordinary packet trait.

A direct OS UDP socket must not instantiate IP packet routing code. A plain
`IpPacketSocket` must not carry UDP adapter branches. A socket that does not
need a doorbell gets an inlined no-op `notify_tx`.

Choices fixed at socket construction belong in associated types or generic
parameters, not runtime enums. Examples include `IpFamily`, polling drivers,
egress handles, and packet policies.

Copy boundaries must be visible. OS UDP receive copies packet bytes into
pool-owned buffers. XDP live receive can wrap UMEM frames directly. Relocating
prepend or append may copy when a layout lacks space. Backend-local batching is
welcome when it preserves the trait contract, such as AF_XDP descriptor bursts
that still report prefix-ordered send acceptance.

## Non-Goals

The current design does not provide:

- API compatibility with any earlier socket crate.
- A single trait object that erases all backend differences in the hot path.
- Treating Ethernet frames as the main public IP packet socket boundary.
- SmartNIC match/action programming or flow offload APIs.
- A DPDK backend before there is enough concrete implementation pressure to
  design it well.

The book records these choices and should change when implementation experience
changes the design.
