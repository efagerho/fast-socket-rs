# XDP Factory

The `XdpFactoryBuilder` is the high-level way to build AF_XDP sockets. It runs
in two phases (matching the `Send` builder / `!Send` live socket split):

1. **Phase 1 — any thread.** Discover the interface's queues, attach the eBPF
   program once per interface, fill the port filter, and partition the claimed
   queues into `threads(T)` contiguous blocks. Produces a `Vec<XdpWorkerPlan>`
   — one plan (one aggregate socket) per worker thread. Plans are `Send`.
2. **Phase 2 — per worker thread.** Move one plan to a thread and call an
   `open_*` opener. The opener pins the thread to `plan.cpu()` (the lowest member
   IRQ CPU) and opens that worker's `XdpUdpAggregate` / `XdpIpPacketAggregate` —
   all member queues sharing one NUMA-local UMEM.

The whole partition is driven by one knob, `threads(T)`: `T` must divide the
claimed queue count `Q`. `threads(1)` is one aggregate over every claimed queue,
`threads(Q)` is one single-queue socket per queue, and any value in between fans
`Q/T` queues into each worker. If `threads` is omitted it defaults to `Q`.

> One shared UMEM binds to one netdev, so every queue in a block must be on the
> same interface. On a bond, choose a `threads` value whose blocks stay within a
> single slave (or run one factory per slave); a block that would span slaves is
> a `build()` error.

## Sender: blast across `--threads` aggregates

Each worker thread owns one aggregate socket and round-robins transmit across
its member queues:

```rust,ignore
use fast_socket_xdp_rs::{InterfaceSelector, PortFilter, RouteSnapshot, XdpFactoryBuilder};

let routes = RouteSnapshot::from_netlink()?;

// Phase 1: discover queues, attach the program, partition into `threads` plans.
let factory = XdpFactoryBuilder::new(InterfaceSelector::Name("eth0".into()))?
    .threads(threads)
    .port_filter(PortFilter::UdpPorts(vec![local.port()]))
    .route_snapshot(routes)
    .build()?;

let mut handles = Vec::new();
for plan in factory.into_worker_plans() {
    handles.push(std::thread::spawn(move || -> std::io::Result<()> {
        // Phase 2: pins to plan.cpu(); one aggregate over this worker's queues.
        let mut aggregate = plan.open_udp_busy_poll(local)?;
        let member_count = aggregate.len();
        let mut next = 0;
        loop {
            let socket = &mut aggregate.members_mut()[next];
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
let factory = XdpFactoryBuilder::new(InterfaceSelector::Name("eth0".into()))?
    .threads(threads)
    .port_filter(PortFilter::UdpPorts(vec![bind.port()]))
    .route_snapshot(routes.clone())
    .build()?;

for plan in factory.into_worker_plans() {
    std::thread::spawn(move || -> std::io::Result<()> {
        let mut aggregate = plan.open_ip_packet_busy_poll()?; // pins to plan.cpu()
        let mut rx = RecvBatch::with_capacity(64);
        loop {
            for socket in aggregate.members_mut() {
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
