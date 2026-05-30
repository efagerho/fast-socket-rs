# Custom Router

The `custom-router` example demonstrates AF_XDP UDP construction with a
user-provided router, built through the factory. It sends one fixed 64-byte UDP
payload per second per queue to `--target`, but it does not use the Linux route
and neighbor tables for transmit egress. Instead, every destination resolves
through a route table whose next hop
is `0.0.0.0`, and an ARP table that returns the `--mac` address for every
next-hop lookup.

Run it with:

```bash
cargo run -p fast-socket-examples --bin custom-router -- \
  --device eth0 \
  --target 192.0.2.10:9000 \
  --mac 02:00:00:00:00:01
```

## Routing Tables

The route table is a small `RouteTable<V4Only>` implementation. It ignores the
destination IP and always returns the AF_XDP-bound interface with `0.0.0.0` as
the next hop.

```rust,ignore
#[derive(Clone, Copy, Debug)]
struct DefaultRouteTable {
    ifindex: IfIndex,
}

impl RouteTable<V4Only> for DefaultRouteTable {
    fn resolve_route(&self, _dst: Ipv4Addr) -> Option<RouteHop<Ipv4Addr>> {
        Some(RouteHop {
            ifindex: self.ifindex,
            next_hop: Ipv4Addr::UNSPECIFIED,
        })
    }
}
```

The ARP table is a matching `NeighborTable<V4Only>` implementation. It ignores
the next-hop IP and returns the MAC address supplied on the command line.

```rust,ignore
#[derive(Clone, Copy, Debug)]
struct ConstantArpTable {
    mac: LinkAddr,
}

impl NeighborTable<V4Only> for ConstantArpTable {
    fn resolve_l2(&self, _next_hop: Ipv4Addr) -> Option<LinkAddr> {
        Some(self.mac)
    }
}
```

The UDP router composes those two tables into an `XdpEgress`. The egress still
uses the socket's queue-local interface and queue from `XdpRouteContext`, plus
the source MAC and MTU read during setup.

```rust,ignore
#[derive(Clone, Copy, Debug)]
struct CustomRouter {
    routes: DefaultRouteTable,
    arp: ConstantArpTable,
    src_mac: LinkAddr,
    mtu: u32,
}

impl XdpUdpRouter for CustomRouter {
    fn resolve_udp_egress(&self, dst: Ipv4Addr, context: XdpRouteContext) -> Option<XdpEgress> {
        let route = self.routes.resolve_route(dst)?;
        if route.ifindex != context.ifindex {
            return None;
        }

        let dst_mac = self.arp.resolve_l2(route.next_hop)?;
        Some(XdpEgress::ipv4(
            context.ifindex,
            context.queue,
            dst_mac,
            self.src_mac,
            self.mtu.min(context.mtu as u32),
        ))
    }
}
```

## Socket Setup

`main` builds a factory over the device, takes one worker plan, builds the
custom router from the plan's interface, and opens the worker's aggregate with
`open_udp_busy_poll_with_router` — the factory opener that accepts a
caller-supplied `XdpUdpRouter` (the default openers use `XdpQueueLocalRouter`).
The opener pins the thread to `plan.cpu()` and shares one UMEM across the
aggregate's queues.

This example still starts `XdpRouteMonitor` and registers one update handle per
aggregate member so the XDP setup has the same live-monitoring shape as the
default router examples. The demo router ignores those handles because it uses
the static `--mac` egress supplied by the operator; real custom routers should
use monitor updates to rebuild or invalidate any cached egress.

```rust,ignore
let local = SocketAddrV4::new(interface_ipv4_addr(&args.device)?, dynamic_source_port());
let mtu = interface_mtu(&args.device)?;
let src_mac = interface_mac(&args.device)?;

let factory = XdpFactoryBuilder::new(InterfaceSelector::Name(args.device.clone()))?
    .threads(args.threads)
    .port_filter(PortFilter::UdpPorts(vec![local.port()]))
    .mtu(mtu as usize)
    .build()?;

let plans = factory.into_worker_plans();
let monitor_queue = plans
    .first()
    .and_then(|plan| plan.queue_ids().first())
    .copied()
    .unwrap_or_else(|| QueueId::new(0));
let mut route_monitor = XdpRouteMonitor::new();

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

for (plan, route_updates) in workers {
    let router = CustomRouter {
        routes: DefaultRouteTable { ifindex: plan.ifindex() },
        arp: ConstantArpTable { mac: args.mac },
        src_mac,
        mtu,
    };
    let _route_updates = route_updates; // keep handles alive; custom refresh policy owns use.
    // Pins to plan.cpu(); one aggregate socket over this worker's queues.
    let mut aggregate = plan.open_udp_busy_poll_with_router(local, || router)?;
    // ... spawn a worker that sends across aggregate.members_mut() ...
}
```

## Main Loop

Each worker loop sends one packet per member queue, then sleeps for one second
until a shutdown signal arrives. `aggregate.members_mut()` yields the worker's
queues; reflection/transmit on each member stays on that member's shared-UMEM
frame slice.

```rust,ignore
let payload = payload();

while !shutdown_requested() {
    for socket in aggregate.members_mut() {
        send_one(socket, target, &payload)?;
        socket.drain_tx_completions()?;
    }
    thread::sleep(SEND_INTERVAL);
}
```

`send_one` uses the generic `UdpSocket` transmit path. It allocates one TX
buffer, writes the fixed payload, freezes it, wraps it in `UdpTransmit`, and
submits a single-slot batch.

```rust,ignore
fn send_one<S>(socket: &mut S, target: SocketAddr, payload: &[u8; PAYLOAD_LEN]) -> Result<(), BoxError>
where
    S: UdpSocket,
{
    let mut tx_buffers = Vec::with_capacity(1);
    loop {
        tx_buffers.clear();
        if socket.allocate_tx_batch(&mut tx_buffers, 1)? == 0 {
            socket.drain_tx_completions()?;
            std::hint::spin_loop();
            continue;
        }

        let mut packet = tx_buffers.pop().expect("one TX buffer was allocated");
        packet.extend_from_slice(payload)?;
        let mut batch = [TxSlot::Ready(UdpTransmit::new(packet.freeze(), target))];
        match socket.send(&mut batch)? {
            1 => {
                socket.drain_tx_completions()?;
                return Ok(());
            }
            0 => {
                socket.drain_tx_completions()?;
                std::hint::spin_loop();
            }
            _ => unreachable!("single-packet batch cannot accept more than one packet"),
        }
    }
}
```

## Caching Egress for Bursts and Small Peer Sets

The default `XdpQueueLocalRouter` resolves each packet through a queue-local
route snapshot. Gateway routes already carry precomputed `XdpResolvedEgress`,
including materialized L2 header bytes, so the default router avoids rebuilding
Ethernet headers on the UDP hot path. It still performs the route match and
selects the matching precomputed entry for each packet. Any server that sends
many packets in a row to the same destination, or that communicates with only a
small set of peers, can move that remaining lookup work into flow setup by
using a custom router as a memoization layer.

The pattern is always the same: resolve egress *once* using the real route
snapshot when a peer becomes known, store the precomputed `XdpResolvedEgress`,
and serve it from the router on every packet. A router that only implements
`resolve_udp_egress` stays compatible, but it will rebuild the L2 bytes through
the trait's default adapter. Override `resolve_udp_egress_resolved` when the
router owns a resolved value.

For long-lived caches built from Linux route state, keep the same netlink
monitor thread shown in the [XDP Factory](xdp-factory.md) setup and rebuild or
invalidate the custom cache when a new snapshot is published. The built-in
`XdpRouteMonitorHandle` applies directly to `XdpQueueLocalRouter`; custom
routers own their refresh policy, but the setup code should still start the
monitor so the refresh source exists.

### Single destination

When the server talks to exactly one peer (a log shipper writing to a single
collector, a market-data publisher feeding one multicast group, a metrics
agent reporting to one telemetry endpoint), the router is one field read:

```rust,ignore
#[derive(Clone, Copy, Debug)]
struct SingleDestinationRouter {
    egress: XdpResolvedEgress,
}

impl XdpUdpRouter for SingleDestinationRouter {
    fn resolve_udp_egress(&self, _dst: Ipv4Addr, _context: XdpRouteContext) -> Option<XdpEgress> {
        Some(self.egress.egress())
    }

    fn resolve_udp_egress_resolved(
        &self,
        _dst: Ipv4Addr,
        _context: XdpRouteContext,
    ) -> Option<XdpResolvedEgress> {
        Some(self.egress)
    }
}
```

Resolve once per worker at setup using the real Linux tables and hand the
precomputed value to every packet (shown for a single-queue plan; with multiple
queues per worker, resolve a per-queue egress for each of `plan.queue_ids()`):

```rust,ignore
let routes = RouteSnapshot::from_netlink()?;
let plan = factory.into_worker_plans().pop().expect("one worker plan");
let mut route_monitor = XdpRouteMonitor::new();
let route_updates = plan
    .queue_ids()
    .iter()
    .map(|_| route_monitor.register_queue())
    .collect::<Vec<_>>();
let queue = plan.queue_ids()[0];
let _route_monitor_thread =
    route_monitor.start_netlink(queue, Duration::from_secs(1));
let egress = routes
    .egress_v4_for_interface(*target.ip(), plan.ifindex(), queue)
    .map(XdpResolvedEgress::from_egress)
    .ok_or("no route to target")?;

let _route_updates = route_updates; // keep handles alive; custom refresh policy owns use.
let mut aggregate =
    plan.open_udp_busy_poll_with_router(local, || SingleDestinationRouter { egress })?;
```

### Handful of peers

When the destination set is small but not singleton (a game server pushing
state updates to a few dozen clients, an L4 load balancer fanning out to a
known backend pool, a QUIC server with a bounded active-connection count),
a tiny `Vec` of `(Ipv4Addr, XdpResolvedEgress)` entries beats every hashed lookup:
the entries fit in L1, the compare is one `u32` equality, and branch
prediction is perfect after the first iteration.

```rust,ignore
#[derive(Clone, Debug)]
struct PeerCacheRouter {
    peers: Vec<(Ipv4Addr, XdpResolvedEgress)>,
}

impl XdpUdpRouter for PeerCacheRouter {
    fn resolve_udp_egress(&self, dst: Ipv4Addr, context: XdpRouteContext) -> Option<XdpEgress> {
        self.resolve_udp_egress_resolved(dst, context)
            .map(|egress| egress.egress())
    }

    fn resolve_udp_egress_resolved(
        &self,
        dst: Ipv4Addr,
        _context: XdpRouteContext,
    ) -> Option<XdpResolvedEgress> {
        self.peers
            .iter()
            .find(|(ip, _)| *ip == dst)
            .map(|(_, egress)| *egress)
    }
}
```

Populate the peer list at flow setup using the real route snapshot — the
optimization is to do that work once per peer, not once per packet:

```rust,ignore
let routes = RouteSnapshot::from_netlink()?;
let mut route_monitor = XdpRouteMonitor::new();
let route_updates = plan
    .queue_ids()
    .iter()
    .map(|_| route_monitor.register_queue())
    .collect::<Vec<_>>();
let queue = plan.queue_ids()[0];
let _route_monitor_thread =
    route_monitor.start_netlink(queue, Duration::from_secs(1));
let peers = peer_addrs
    .into_iter()
    .map(|addr| {
        let egress = routes
            .egress_v4_for_interface(addr, plan.ifindex(), queue)
            .map(XdpResolvedEgress::from_egress)
            .ok_or_else(|| format!("no route to {addr}"))?;
        Ok((addr, egress))
    })
    .collect::<Result<Vec<_>, BoxError>>()?;
let _route_updates = route_updates; // keep handles alive; custom refresh policy owns use.
let mut aggregate =
    plan.open_udp_busy_poll_with_router(local, || PeerCacheRouter { peers: peers.clone() })?;
```

If the peer count grows past the point where a linear scan stops fitting in
L1, replace the `Vec` with a `HashMap<Ipv4Addr, XdpResolvedEgress>` built on a
faster hasher than the standard library default (for example
`rustc_hash::FxHashMap`). The application still owns when entries are added
and evicted, so the hot path stays predictable.

### Where the wins come from

Both routers avoid the work the default router does per packet:

* No longest-prefix-match scan over the IPv4 route table.
* No scan over the snapshot's precomputed gateway egress entries.
* No per-packet L2 materialization, because the router returns
  `XdpResolvedEgress` directly.
* For direct routes, no neighbor or interface lookup, since source MAC,
  destination MAC, MTU, and L2 bytes are baked into the cached value.

What remains in the send path is a struct copy (for the single-destination
case) or a short equality scan (for the peer cache). On the blast workload
that work falls to roughly 0% of CPU; on real workloads it shifts cycles
back to the application without changing the public socket API.

### When not to use this pattern

A precomputed cache is worse than the default router when destinations are
effectively random — DNS responders, NAT boxes, scanners, anything where
the destination rarely repeats inside the working set. Every miss pays
both the cache lookup and the real route resolution. Keep the default
router for those workloads, or build a hybrid that falls back to the real
`RouteSnapshot` on miss and only inserts into the cache for durable
flows.
