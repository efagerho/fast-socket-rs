# Goals and Non-Goals

Fast Socket exists to make high-rate packet I/O explicit without forcing every
caller to care about one specific backend. The public API should feel like one
design, while still letting each backend keep the representation that makes it
fast.

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

The zero-overhead requirement is a design constraint, not a slogan. It means
unused abstractions must not leak measurable cost into steady-state packet
paths.

- use static dispatch for steady-state packet operations;
- avoid trait objects in send, receive, completion, and polling loops unless a
  caller explicitly chooses them;
- keep optional features represented by concrete policy and metadata types;
- avoid packet-byte copies except at explicit relocation or backend copy
  boundaries;
- make batch operations preserve ownership without hidden allocation;
- keep completion draining explicit for zero-copy transmit paths;
- keep device control and statistics off the ordinary packet trait.

A direct OS UDP socket should not instantiate IP packet routing code. A plain
`IpPacketSocket` should not carry UDP adapter branches. A socket that does not
need a doorbell should get an inlined no-op `notify_tx`.

When a choice is fixed by socket construction, it should be represented by
associated types or concrete generic parameters rather than runtime enums.
`IpFamily`, polling drivers, egress handles, and packet policies are examples.

Copy boundaries must be visible. OS UDP receive copies packet bytes into
pool-owned buffers. XDP live receive can wrap UMEM frames directly. Relocating
prepend or append may copy when a layout lacks space. Backend-local batching is
welcome when it preserves the trait contract, such as AF_XDP descriptor bursts
that still report prefix-ordered send acceptance.

## Non-Goals

The current design intentionally does not try to provide:

- API compatibility with any earlier socket crate.
- A single trait object that erases all backend differences in the hot path.
- Treating Ethernet frames as the main public IP packet socket boundary.
- SmartNIC match/action programming or flow offload APIs.
- A DPDK backend before there is enough concrete implementation pressure to
  design it well.

The book should record the reasoning behind these choices. When implementation
experience changes the design, the docs should change with it.
