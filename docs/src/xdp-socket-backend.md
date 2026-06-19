# XDP Socket Backend

The `fast-socket-xdp-rs` crate provides the Linux AF_XDP backend. It implements
the core `IpPacketSocket` and `UdpSocket` traits over NIC RX/TX queues, UMEM
frames, and an attached XDP redirect program. The public socket types stay close
to the core API: applications allocate packet buffers from socket-owned pools,
receive and transmit in batches, drain completions explicitly, and drive each
live socket from one owner thread.

The backend has two main entry points:

- `XdpFactoryBuilder`, which discovers queue slots, attaches the XDP program,
  partitions queues into worker plans, and opens aggregate UDP sockets.
- `XdpIpPacketSocketBuilder` and `XdpUdpSocketBuilder`, which construct a
  single queue-local socket when the caller wants to manage queue selection and
  program setup directly.

Factory-built aggregates are the usual shape for applications. A worker plan
opens one `XdpUdpAggregate` or `XdpIpPacketAggregate`; each aggregate owns one
member socket per assigned NIC queue and drives those members from the same
worker. The aggregate constructors use a shared UMEM for their members, while
each member keeps its own RX, TX, fill, and completion rings.

## Queue Ownership

AF_XDP binds each live socket to a concrete `(ifindex, queue_id)` pair. The XDP
backend reflects that in its ownership model: a single-queue socket is
queue-local, and an aggregate is just a small scheduler over several
single-queue sockets.

`XdpFactoryBuilder` performs the deployment work before packets flow. It
discovers bindable queue slots for the selected interface, applies the queue
claim, attaches one XDP program per interface index, fills the configured UDP
port filter, groups queues by NUMA node, and produces `XdpWorkerPlan` values.
Opening a pinned worker plan pins the current thread to the selected CPU before
creating the aggregate. The unpinned openers leave placement to the caller or to
an async runtime.

For a one-queue worker, the aggregate has one member and behaves like the
underlying socket. For a multi-queue worker, aggregate receive makes one
round-robin sweep over members and appends packets until the caller's batch is
full or every member has been visited. Aggregate transmit exposes a round-robin
member chooser so applications can spread outgoing packets across TX rings, or
they can work with `members_mut()` directly when a forwarding path must send a
packet on the same queue that received it.

## UMEM Buffers

Live XDP sockets use UMEM-backed packet buffers. RX descriptors wrap frames that
the NIC filled, and TX buffers are handed out from the socket's transmit frame
free list. The fallback heap mode exists for tests and does not represent the
live AF_XDP data path.

XDP packet buffers are `Send`, so an application may allocate a TX buffer on the
socket owner thread, fill it on another thread, and return it to the owner for
transmit. To keep the hot path allocation-free, live buffers store raw pointers
into socket-owned UMEM and reclaim state instead of cloning reference-counted
handles per packet. This relies on the core lifetime contract: every socket and
pool must outlive all buffers it hands out. Debug builds, and release builds with
the `buffer-guard` feature, check this with an owner-generation token.

Frame reclaim is owner-thread optimized. A buffer dropped on the socket owner
thread returns directly to the local free list. A buffer dropped from another
thread enters a bounded remote reclaim queue that the owner drains before
reusing frames. Transmit completions are explicit because an AF_XDP TX frame
cannot return to the pool until the kernel reports that the descriptor has
completed.

## Packet Paths

The IP packet socket receives raw Ethernet frames from AF_XDP, parses the link
and IP headers, and returns `IpPacketReceive` values with backend metadata. Its
transmit path accepts packet buffers with resolved XDP egress data and writes
descriptors to the TX ring.

The UDP socket layers protocol handling on top of the IP packet socket. On
receive, it parses Ethernet, IPv4, and UDP in one backend pass, filters by the
socket's accepted destination ports, and exposes the UDP payload through
`UdpReceive`. Fragmented IP packets are rejected because the backend does not
perform reassembly.

On transmit, the UDP socket asks its router for an `XdpRouteContext`-specific L2
egress result, writes Ethernet, IPv4, and UDP headers into the packet frame, and
submits the frame through the selected TX ring. `notify_tx` rings the AF_XDP
doorbell only when the ring reports `XDP_RING_NEED_WAKEUP`, so busy rings avoid
unnecessary syscalls.

Prepared UDP endpoints provide the fastest repeated-send path for a fixed
target. `XdpUdpEndpoint` caches an L2+IPv4+UDP header template and
`XdpUdpEndpointBatchBuilder` writes that template plus caller payloads directly
into UMEM-backed TX frames, skipping the generic endpoint transmit slot
materialization path.

## Program And Port Filters

The backend attaches the bundled eBPF object through `XdpProgramHandle`. The
object contains two redirect programs:

- the bound-ports program, selected by `udp_ports` or `PortFilter::UdpPorts`,
  redirects UDP packets whose destination port is enabled in the `BOUND_PORTS`
  map;
- the port-range program, selected by `udp_port_range` or
  `PortFilter::UdpPortRange`, redirects packets whose UDP destination port is in
  the configured inclusive range.

Both programs leave unrelated traffic on the kernel path. Queue sockets are
registered in the program's XSKMAP by queue id, and live socket drop removes the
queue entry from that map before the AF_XDP fd goes away. The in-process program
registry rejects attempts to reuse an already-attached program with a different
attach mode, object hash, or program configuration.

## Routing

XDP transmit needs backend-specific egress state: output interface, queue,
effective MTU, next-hop MAC address, and the serialized Ethernet header. The
default UDP router is `XdpQueueLocalRouter`, backed by `XdpLocalRoutes`.

`RouteSnapshot::from_netlink()` captures Linux route, link, and neighbor state
for initial setup. Queue-local routers keep the hot path immutable and local to
the worker, while `XdpRouteMonitor` can publish cold-path snapshot updates to
many queue owners. A queue owner applies updates outside the packet path, which
increments the router generation and invalidates prepared endpoint header caches
when needed.

Applications that already know their egress state can implement `XdpUdpRouter`
directly. The most important fast-path hook is `resolve_udp_l2`, which can
return borrowed prebuilt L2 bytes for the destination instead of rebuilding the
header for every packet.

## Polling Modes

The same socket state supports busy-poll and wait-driven drivers.
`BusyPollXdpUdpSocket` and `BusyPollXdpIpPacketSocket` are intended for tight
worker loops that call receive, transmit, completion draining, and maintenance
methods directly.

`WaitDrivenXdpUdpSocket` and `WaitDrivenXdpIpPacketSocket` expose an AF_XDP fd
through their `PollDriver`. The driver implements `wait()` for readiness waits
and `wake_handle()` for integration with reactors such as the Tokio actor
adapter. Wait-driven sockets remain single-owner objects; runtimes should drive
them from local tasks or actors rather than moving the live socket between
threads.

## Limitations

The backend is Linux-only and requires the privileges needed to create AF_XDP
sockets and load XDP programs. It currently handles IPv4 UDP on the high-level
UDP path; lower-level IP packet sockets can be used for raw packet workflows.
The live data path assumes one owner thread per socket or aggregate member, and
it depends on the application keeping sockets alive until all borrowed pool
buffers have been returned.
