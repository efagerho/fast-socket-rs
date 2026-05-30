# AF_XDP Backend

The AF_XDP backend lives in `fast-socket-xdp-rs` and implements
`IpPacketSocket` and `UdpSocket`. `XdpIpPacketSocket` exposes complete IP
datagrams. `XdpUdpSocket` exposes UDP payloads while using the same AF_XDP
queue, UMEM, egress, completion, and doorbell machinery.

`XdpIpPacketSocketConfig` describes one queue socket:

- interface index and queue id;
- optional NUMA node and hugepage preference;
- receive and transmit `BufferLayout`;
- AF_XDP ring sizes;
- copy or zero-copy mode;
- IP-layer MTU;
- UMEM frame count;
- XDP attach mode and program bytes;
- optional pre-attached XDP program to reuse;
- optional UDP port binding for filtered redirect programs.

Live sockets validate AF_XDP ring sizes at open. FILL, COMPLETION, RX, and TX
capacities must be non-zero powers of two, matching kernel requirements instead
of relying on debug-only assertions.

Live setup validates UMEM sizing and placement. Frame size and frame count are
rounded up to powers of two with checked arithmetic; overflow or invalid UMEM
geometry fails construction. Before allocation, the backend resolves the
interface name from the ifindex and reads the device NUMA node from sysfs. A
configured NUMA node must match the device-reported node. If sysfs cannot report
a usable node, an explicit configured node is accepted.

Live UMEM allocation binds the anonymous mapping to the selected NUMA node
before first touch, then verifies page placement. Hugepage allocation follows
the configured preference; fallback pages must still land on the selected node.

The default layouts use fixed 4096-byte chunks with 2048-byte alignment. RX
reserves backend L2 headroom. TX reserves both backend L2 headroom and public
headroom so IP/UDP or tunnel headers can be prepended before the backend
prepends Ethernet.

`RawXdpSocket` owns the low-level AF_XDP fd and ring mappings. Live sockets
reuse a matching pre-attached program or load the configured program, allocate
UMEM, prefill RX frames, configure FILL, COMPLETION, RX, and TX rings, bind the
socket, register the fd in the XDP program's `XSKMAP`, optionally bind one UDP
destination port, and recycle frames through queue-local reclaim lists. The raw
socket has scalar helpers for one-at-a-time operations and bulk helpers for
burst work. RX, COMPLETION, FILL, and TX paths reserve cursor ranges and copy
descriptors or frame addresses in wrap-aware chunks.

`RawXdpSocket::new` registers the UMEM and binds normally;
`RawXdpSocket::new_shared_umem` binds a member against an already-registered
UMEM with `XDP_SHARED_UMEM` (skipping `XDP_UMEM_REG`) — the shared-UMEM path
used by aggregate sockets. A shared-member bind sets **only**
`XDP_SHARED_UMEM`: it must not also set `XDP_USE_NEED_WAKEUP` or a copy/zero-copy
mode flag (the kernel rejects that with `EINVAL`), so a member inherits the
owner's mode and need-wakeup setting. `RawXdpSocket::configure_busy_poll`
applies the per-fd `SO_PREFER_BUSY_POLL`, `SO_BUSY_POLL`, and
`SO_BUSY_POLL_BUDGET` setsockopts.

Live sockets split UMEM frames into RX-owned and TX-owned regions. Completion
drain validates descriptor addresses, normalizes them to frame starts, and
returns RX-origin frames to FILL while TX-origin frames return to the TX pool.
This protects reflected packets: a packet received in an RX frame and then
transmitted must not poison the TX pool when completion arrives.

IP packet receive drains RX descriptors, parses Ethernet or VLAN headers,
rejects descriptor ranges outside their UMEM frame, accepts IPv4 and IPv6
frames, drops IP fragments, wraps the UMEM frame as an `XdpPacketBufMut`, and
exposes only the IP datagram to `IpPacketSocket::recv`.

`XdpUdpSocket` is constructed through a UDP-level builder from interface, queue,
local IPv4 UDP address, and router configuration. On live receive it parses
Ethernet, IPv4, and UDP in one pass, filters by local address and port, wraps
only the UDP payload, and returns `UdpReceive` with `UdpRecvMeta`. On transmit it
resolves each remote address through its router, validates queue-local egress,
builds IPv4 and UDP headers, prepares the Ethernet or VLAN header, and submits
through the AF_XDP TX path. The live UDP path overrides `allocate_tx_batch` so
callers can obtain multiple TX buffers with one reclaim-list batch. The current
direct UDP implementation is IPv4-only and reports default UDP capabilities.

Live IP packet transmit validates the egress interface and queue, checks MTU and
ethertype, prepares a valid prefix up to TX ring availability, prepends the
Ethernet or VLAN header from `XdpEgress`, enqueues TX descriptors with the bulk
ring path, wakes TX when required, and preserves prefix-accept send semantics.
Live send and allocation paths drain TX completions before work when in-flight
TX pressure crosses the configured threshold; callers can still call
`drain_tx_completions()` explicitly.

Both `XdpIpPacketSocket` and `XdpUdpSocket` implement `RawDevice`, so callers
can read interface facts, the backing NIC queues (`nic_queues()`), per-queue
affinity, per-queue NUMA placement, MTU, and packet-path counters from either
socket shape. `socket_id()` identifies the logical socket separately from those
queues. Live sockets report the resolved UMEM NUMA node; first-pass sockets
report the configured hint when provided.

## Aggregate sockets and shared UMEM

`XdpUdpAggregate` and `XdpIpPacketAggregate` are *logical* sockets fed by 1..N
NIC queues. Each owns one single-queue socket per claimed queue and multiplexes
work across them: `recv` sweeps members round-robin (RX fan-in), transmit
spreads across members, and `drain_tx_completions` drains every member. A
single-queue socket is the `members == 1` case.

The members of one aggregate share **one UMEM**, allocated NUMA-local and
registered once: member 0 is the owner (`RawXdpSocket::new`), members 1..N bind
it with `RawXdpSocket::new_shared_umem`. The UMEM is partitioned into one
disjoint frame slice per member, so each member runs the proven single-queue
RX/TX path over its own frames while the DMA region is allocated and registered
exactly once. Because one UMEM binds to one netdev, every member of an aggregate
must be on the same interface (the factory enforces this).

## Two-phase factory

`XdpFactoryBuilder` -> `XdpFactory` -> `XdpWorkerPlan` builds aggregate sockets
in two phases, matching the `Send` builder / `!Send` live socket split:

- **Phase 1 (any thread).** `XdpFactoryBuilder::new(InterfaceSelector)` discovers
  the interface's queue slots. `claim(QueueClaim)` chooses which queues (`All`,
  `First(n)`, or an explicit `Queues(..)` set); `port_filter(PortFilter)` selects
  the redirect filter (`AllIp`, or `UdpPorts(..)` bound into the program);
  `threads(T)` sets the worker count. `claimed_queue_count()` and
  `irq_cpu_count()` are readable after `claim` to compute `T`. `build()` attaches
  one program per interface, fills the filter, validates that `T` divides the
  claimed queue count, and partitions the claim-order queues into `T` contiguous
  single-interface blocks — one `XdpWorkerPlan` (one aggregate socket) per block.
- **Phase 2 (per worker thread).** Move one `Send` `XdpWorkerPlan` to a thread
  and call `plan.open_udp_busy_poll(local)` /
  `plan.open_ip_packet_busy_poll()` (or `open_udp_busy_poll_with_router` for a
  custom `XdpUdpRouter`). Each opener pins the thread to `plan.cpu()` (the lowest
  member IRQ CPU) before allocating, so the UMEM, rings, and scratch are
  NUMA-local; `open_*_unpinned` variants skip pinning for custom placement.
  `plan.numa_node()` and `plan.queue_ids()` expose the block's placement.

So the worker-thread count is the single knob: `threads(1)` is one aggregate over
every claimed queue on an interface, `threads(Q)` is one single-queue socket per
queue, and intermediate values fan `Q/T` queues into each worker. A block that
would span interfaces (for example `threads(1)` across a bond's two slaves) is a
`build()` error, since one shared UMEM binds to one netdev.

The embedded eBPF program is closed by default: with no UDP ports bound it
returns `XDP_PASS` for every frame so attaching the program alone does not
hijack TCP, ICMP, ND, or other kernel-path traffic. Userspace opts a
destination port into AF_XDP redirection by calling `bind_port(port)`, which
populates `BOUND_PORTS` and `BOUND_PORT_COUNT`. Once at least one port is
bound, the program redirects only matching non-fragmented IPv4 UDP packets to
`XSKMAP[rx_queue_index]`; IPv6, non-UDP IPv4, malformed packets, and non-IP
link traffic continue to take the kernel path so Linux route, neighbor, and
ARP state stay available to the userspace netlink resolver. The L2 parser
accepts untagged frames, single 802.1Q tags (`0x8100`), and 802.1ad QinQ
(`0x88a8` outer with a `0x8100` inner). When a packet passes the bound-port
filter but `XSKMAP::redirect` fails (no socket registered for the queue, or
the kernel rejected the redirect), the program returns `XDP_DROP` and
increments `DROP_COUNTERS[DROP_REASON_XSKMAP_MISS]` or
`DROP_COUNTERS[DROP_REASON_REDIRECT_ERROR]` so operators can observe
misconfigurations instead of silently leaking packets to the stack.

Routing uses queue-local snapshots. `RouteSnapshot` is built from netlink route,
neighbor, and link dumps. UDP sockets resolve each remote address through an
`XdpUdpRouter`; the default `XdpQueueLocalRouter` owns queue-local route state.
`XdpRouteMonitor` can publish refreshed snapshots to queue owners, and each
queue adopts updates outside its packet path.

For tests and unprivileged bring-up, an in-memory first-pass mode queues
normalized packets and submitted Ethernet frames without opening a live AF_XDP
fd. Both IP packet and UDP socket paths use that mode in unit tests.
