# AF_XDP Backend

The AF_XDP backend lives in `fast-socket-xdp-rs` and implements both
`IpPacketSocket` and `UdpSocket`. `XdpIpPacketSocket` exposes complete IP datagrams.
`XdpUdpSocket` exposes UDP payloads directly while still using the same AF_XDP
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

AF_XDP ring sizes are validated when live sockets open. FILL, COMPLETION, RX,
and TX capacities must be non-zero powers of two, matching kernel AF_XDP ring
requirements rather than relying on debug-only assertions.

Live setup also validates UMEM sizing and placement. The computed frame size and
requested frame count are rounded up to powers of two with checked arithmetic,
and overflow or invalid UMEM geometry fails during construction. Before
allocating UMEM, the backend resolves the interface name from the configured
ifindex and reads the backing device NUMA node from sysfs. A configured NUMA
node must match the device-reported node. If sysfs cannot report a usable node,
an explicit configured NUMA node is accepted as the fallback.

Live UMEM allocation binds the anonymous mapping to the selected NUMA node
before first touch, then verifies page placement. Hugepage allocation is tried
according to the configured hugepage preference; fallback pages must still land
on the selected node.

The default layouts use fixed 4096-byte chunks with 2048-byte alignment. RX
reserves backend L2 headroom. TX reserves both backend L2 headroom and public
headroom so IP/UDP or tunnel headers can be prepended before the backend
prepends Ethernet.

`RawXdpSocket` owns the low-level AF_XDP fd and ring mappings. Live sockets
reuse a matching pre-attached program or load the configured program, allocate
UMEM, prefill RX frames, configure FILL, COMPLETION, RX, and TX rings, bind the
socket, register the fd in the XDP program's `XSKMAP`, optionally bind one UDP
destination port, and recycle frames through queue-local reclaim lists.
The raw socket has scalar descriptor helpers for one-at-a-time operations and
bulk helpers for burst work. RX, COMPLETION, FILL, and TX paths can reserve
cursor ranges and copy descriptors or frame addresses in wrap-aware chunks.

Live sockets split UMEM frames into RX-owned and TX-owned regions. Completion
drain validates descriptor addresses, normalizes them back to frame starts, and
returns completed RX-origin frames to the FILL path, while TX-origin frames
return to the TX pool. That matters for reflected packets: a packet received in
an RX frame and then transmitted should not poison the TX pool when the
completion arrives.

IP packet receive drains RX descriptors, parses Ethernet or VLAN headers,
rejects descriptor ranges that escape their UMEM frame, accepts IPv4 and IPv6
frames, drops IP fragments, wraps the UMEM frame as an `XdpPacketBufMut`, and
exposes only the IP datagram to `IpPacketSocket::recv`.

`XdpUdpSocket` wraps an `XdpIpPacketSocket` with a local IPv4 UDP address and a
resolved `XdpEgress`. On live receive it parses Ethernet, IPv4, and UDP in one
backend pass, filters by the local address and port, wraps only the UDP payload,
and returns `UdpReceive` with `UdpRecvMeta`. On transmit it validates the
queue-local egress, builds IPv4 and UDP headers, prepares the Ethernet or VLAN
header once per batch, and submits the frame through the AF_XDP TX path. The
live UDP path also overrides `allocate_tx_batch` so callers can obtain multiple
TX buffers with one reclaim-list batch. The current direct UDP implementation is
IPv4-only and reports default UDP capabilities.

Live IP packet transmit validates the egress interface and queue, checks MTU and
ethertype, prepares a prefix of valid packets up to TX ring availability,
prepends the Ethernet or VLAN header from `XdpEgress`, enqueues TX descriptors
with the bulk ring path, wakes the TX path when required, and preserves the
core prefix-accept send contract. Live send and allocation paths drain TX
completions before work when in-flight TX pressure crosses the configured
threshold; callers can still use `drain_tx_completions()` explicitly.

Both `XdpIpPacketSocket` and `XdpUdpSocket` implement `RawDevice`, so callers
can read interface facts, queue affinity, queue NUMA placement, MTU, and
packet-path counters from either concrete socket shape. Live sockets report the
resolved UMEM NUMA node; first-pass sockets report only the configured hint when
one was provided.

The embedded eBPF program redirects untagged or single-VLAN IPv4 and IPv6 frames
to `XSKMAP[rx_queue_index]` while no UDP ports are bound. When userspace binds
UDP ports, the program uses `BOUND_PORTS` and `BOUND_PORT_COUNT` to redirect
only matching non-fragmented IPv4 UDP packets; unrelated IP traffic, IPv6
traffic, malformed packets, and non-IP link traffic stay on the kernel path.
Passing ARP and other link traffic keeps Linux route and neighbor state
available for the userspace netlink resolver.

Routing uses queue-local snapshots. `RouteSnapshot` is built from netlink route,
neighbor, and link dumps. `XdpRouteMonitor` can publish refreshed snapshots to
queue owners, and each queue adopts updates outside its packet path.

For tests and unprivileged bring-up, the backend also has an in-memory
first-pass mode that queues normalized packets and submitted Ethernet frames
without opening a live AF_XDP fd. Both the IP packet and direct UDP socket paths
use that mode in unit tests.
