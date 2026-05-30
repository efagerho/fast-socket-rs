# XDP Factory

The `XdpFactoryBuilder` is the high-level way to build AF_XDP sockets. It runs
in two phases (matching the `Send` builder / `!Send` live socket split):

1. **Phase 1 — any thread.** Discover the interface's queues, attach the eBPF
   program once per interface, fill the port filter, and partition the claimed
   queues **by NUMA node** (see below). Produces a `Vec<XdpWorkerPlan>` — one plan
   (one aggregate socket) per worker thread. Plans are `Send`.
2. **Phase 2 — per worker thread.** Move one plan to a thread and call an
   `open_*` opener. The opener pins the thread to `plan.cpu()` — a dedicated,
   NUMA-local core — and opens that worker's `XdpUdpAggregate` /
   `XdpIpPacketAggregate`, all member queues sharing one NUMA-local UMEM.

### How queues map to workers

`build()`:

1. maps each claimed queue to the NUMA node it lives on (the node of its IRQ CPU,
   falling back to the interface's device node);
2. splits the `threads` budget evenly across those nodes (`threads / nodes`);
3. within each node, spreads the node's queues evenly over that many **dedicated,
   node-local cores** — one aggregate socket per core.

So `threads` is the knob: `threads(2)` on a single-node 2-slave bond is one
aggregate per slave; larger values fan fewer queues into each (more) workers. If
`threads` is omitted it defaults to the queue count (one queue per worker).

A worker only ever drives queues on its own NUMA node, each worker pins to a
distinct node-local core, and each worker stays single-interface. Under preferred
busy polling NAPI runs inline on the busy-polling core and hard IRQs are
deferred, so `plan.cpu()` is a dedicated node-local core, *not* the queue's IRQ
core — the IRQ core doesn't matter.

`build()` errors if `threads` isn't divisible by the number of NUMA nodes the
queues span, if a node's queue count isn't divisible by its share of threads, or
if a block would span interfaces (one shared UMEM binds one netdev — e.g.
`threads(1)` across a bond's two slaves errors; use `threads(2)` or run one
factory per slave).

## Sender: blast across `--threads` aggregates

Each worker thread owns one aggregate socket and round-robins transmit across
its member queues:

```rust,ignore
use std::time::Duration;

use fast_socket_rs::QueueId;
use fast_socket_xdp_rs::{
    InterfaceSelector, PortFilter, RouteSnapshot, XdpFactoryBuilder, XdpRouteMonitor,
};

let routes = RouteSnapshot::from_netlink()?;
// The snapshot includes precomputed L2 headers for IPv4 gateway routes.
let mut route_monitor = XdpRouteMonitor::new();

// Phase 1: discover queues, attach the program, partition into `threads` plans.
let factory = XdpFactoryBuilder::new(InterfaceSelector::Name("eth0".into()))?
    .threads(threads)
    .port_filter(PortFilter::UdpPorts(vec![local.port()]))
    .route_snapshot(routes)
    .build()?;

let plans = factory.into_worker_plans();
let monitor_queue = plans
    .first()
    .and_then(|plan| plan.queue_ids().first())
    .copied()
    .unwrap_or_else(|| QueueId::new(0));

let mut workers = Vec::with_capacity(plans.len());
for plan in plans {
    // Register one update handle per member socket. Handles remember the last
    // generation they applied, so sharing one handle across members would update
    // only the first member.
    let route_updates = plan
        .queue_ids()
        .iter()
        .map(|_| route_monitor.register_queue())
        .collect::<Vec<_>>();
    workers.push((plan, route_updates));
}

let _route_monitor_thread =
    route_monitor.start_netlink(monitor_queue, Duration::from_secs(1));

let mut handles = Vec::new();
for (plan, mut route_updates) in workers {
    handles.push(std::thread::spawn(move || -> std::io::Result<()> {
        // Phase 2: pins to plan.cpu(); one aggregate over this worker's queues.
        let mut aggregate = plan.open_udp_busy_poll(local)?;
        let member_count = aggregate.len();
        debug_assert_eq!(route_updates.len(), member_count);
        let mut next = 0;
        loop {
            let socket = &mut aggregate.members_mut()[next];
            route_updates[next].apply_updates(socket.routes_mut());
            // allocate_tx_batch + send + drain_tx_completions on this member ...
            next = (next + 1) % member_count;
        }
    }));
}
```

## Receiver / pong: fan-in and per-queue reflect

For an IP-packet or UDP listener, open the IP-packet/UDP aggregate and service
its members. `recv` fans in across queues; reflection sends back on the queue a
frame arrived on (each member owns its shared-UMEM frame slice):

```rust,ignore
let routes = RouteSnapshot::from_netlink()?;
let mut route_monitor = XdpRouteMonitor::new();

let factory = XdpFactoryBuilder::new(InterfaceSelector::Name("eth0".into()))?
    .threads(threads)
    .port_filter(PortFilter::UdpPorts(vec![bind.port()]))
    .route_snapshot(routes.clone())
    .build()?;

let plans = factory.into_worker_plans();
let monitor_queue = plans
    .first()
    .and_then(|plan| plan.queue_ids().first())
    .copied()
    .unwrap_or_else(|| QueueId::new(0));

let mut workers = Vec::with_capacity(plans.len());
for plan in plans {
    let route_updates = plan
        .queue_ids()
        .iter()
        .map(|_| route_monitor.register_queue())
        .collect::<Vec<_>>();
    workers.push((plan, route_updates));
}

let _route_monitor_thread =
    route_monitor.start_netlink(monitor_queue, Duration::from_secs(1));

for (plan, mut route_updates) in workers {
    std::thread::spawn(move || -> std::io::Result<()> {
        let mut aggregate = plan.open_ip_packet_busy_poll()?; // pins to plan.cpu()
        let mut rx = RecvBatch::with_capacity(64);
        debug_assert_eq!(route_updates.len(), aggregate.len());
        loop {
            for (socket, route_update) in aggregate
                .members_mut()
                .iter_mut()
                .zip(route_updates.iter_mut())
            {
                route_update.apply_updates(socket.routes_mut());
                rx.clear();
                socket.recv(&mut rx)?;
                // parse / reflect on this same socket, then drain ...
                socket.drain_tx_completions()?;
            }
        }
    });
}
```

## Choosing `threads`

`claimed_queue_count()` and `irq_cpu_count()` are readable after `claim` (before
the consuming `threads` call) so callers can compute `T`:

```rust,ignore
let builder = XdpFactoryBuilder::new(InterfaceSelector::Name("eth0".into()))?
    .claim(QueueClaim::All);
let cpus = builder.irq_cpu_count()?;     // one aggregate per IRQ CPU
let factory = builder.threads(cpus as usize).port_filter(PortFilter::AllIp).build()?;
```

**Sizing — more threads is not always more throughput.** Each worker
round-robins its queues, overlapping their TX/RX rings so the NIC's completion
latency stays hidden. With only one queue per worker (`threads` == queue count)
there is nothing to overlap: the worker stalls on that queue's completions and
degenerates into a busy-spin, so peak throughput is usually reached with a
*handful* of queues per worker, not one. Tune `threads` to where the NIC
saturates rather than maxing it.

## Escape hatches

If the default placement isn't what you want, there are two override points.

**1. Custom core assignment.** Keep the NUMA grouping and single-interface
blocks, but choose the core yourself. The closure gets the worker's NUMA node,
that node's CPU ids, and the worker's 0-based index on the node, and returns a
core (which must be one of those CPUs):

```rust,ignore
let factory = XdpFactoryBuilder::new(InterfaceSelector::Name("eth0".into()))?
    .threads(8)
    .core_assignment(|_node, cpus, worker_index| cpus[(worker_index * 2) % cpus.len()])
    .build()?;
```

**2. Build plans by hand (bypass the factory).** For full control over queue
grouping and placement, construct `XdpWorkerPlan`s directly and drive them with
the same `open_*` openers:

```rust,ignore
let program = XdpProgramHandle::load(ifindex.get(), AttachMode::Default, None)?;
let config = XdpIpPacketSocketConfig {
    ifindex,
    attached_program: Some(program),
    ..Default::default()
};
let mut route_monitor = XdpRouteMonitor::new();
let queues = vec![QueueId::new(0), QueueId::new(1)];
let mut route_updates = queues
    .iter()
    .map(|_| route_monitor.register_queue())
    .collect::<Vec<_>>();
let _route_monitor_thread =
    route_monitor.start_netlink(queues[0], Duration::from_secs(1));

// queues must all be on config.ifindex; cpu is the core open_* pins to.
let plan = XdpWorkerPlan::new(config, queues, /*cpu*/ 3, numa);
let mut aggregate = plan.open_ip_packet_busy_poll()?;
for (socket, route_update) in aggregate
    .members_mut()
    .iter_mut()
    .zip(route_updates.iter_mut())
{
    route_update.apply_updates(socket.routes_mut());
}
```

(The aggregate openers `XdpUdpAggregate::open_busy_poll` /
`XdpIpPacketAggregate::open_busy_poll` are also public if you don't want plans at
all.)

## Other knobs

- `claim(QueueClaim)` — `All`, `First(n)`, or an explicit `Queues(..)` set
  (flat indices from `xdp_queue_slots_for_interface`).
- `port_filter(PortFilter)` — `AllIp`, or `UdpPorts(..)` bound into the program's
  `BOUND_PORTS`.
- `frame_count` (per member), `mtu`, `rings`, `xdp_mode`, `buffers`,
  `attach_mode`, `route_snapshot`.
- `XdpWorkerPlan::open_udp_busy_poll_with_router(local, make_router)` opens with a
  custom [`XdpUdpRouter`](custom-router.md) instead of the default queue-local
  router; `open_*_unpinned` variants skip the thread pinning for custom
  placement.
