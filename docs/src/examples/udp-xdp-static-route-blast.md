# udp-xdp-static-route-blast

This example exists to showcase escape hatches around the normal UDP socket
path. In particular, it shows how to override the default route lookup by
supplying a router that returns precomputed layer-2 egress state for a known
target.

`udp-xdp-static-route-blast` is an XDP-only packet generator. It resolves one
target through the current netlink route and neighbor snapshot, caches the
resulting layer-2 header, and sends generated UDP payloads as fast as the XDP
workers accept them.

It does not take `--backend`; it always uses busy-poll XDP sockets.

```sh
cargo run -p fast-socket-examples --bin udp-xdp-static-route-blast -- \
  --device eth0 \
  --target 192.168.0.20:9000 \
  --threads 1 \
  --payload-len 64 \
  --batch-size 64 \
  --drain-every-batches 2 \
  --duration-ms 10000
```

Important flags:

- `--target <ipv4:port>` is the remote UDP endpoint.
- `--source-ip <ipv4>` overrides the interface IPv4 address used as the source.
- `--source-port <port>` overrides the generated dynamic source port.
- `--threads <n>` controls how many XDP worker plans the factory builds.
- `--drain-every-batches <n>` controls how often each socket drains TX
  completions while making progress.
- `--duration-ms <ms>` stops the generator after a fixed duration. Without it,
  the process runs until a shutdown signal or worker failure.

The target must be routable through the selected interface, and the route lookup
must find neighbor information for the target or next hop.

## Custom Route Table

The example uses `StaticTargetRouter` as a one-entry route table. It stores the
only destination the generator will send to and the fully resolved XDP egress
state for that destination:

```rust
#[derive(Clone, Debug)]
struct StaticTargetRouter {
    target: Ipv4Addr,
    resolved: XdpResolvedEgress,
}
```

`XdpUdpRouter` is the socket's route lookup hook. The default XDP UDP sockets
use a queue-local router backed by the current route snapshot. This example
replaces that router with one that only answers for the configured target and
interface:

```rust
impl XdpUdpRouter for StaticTargetRouter {
    fn resolve_udp_egress(&self, dst: Ipv4Addr, context: XdpRouteContext) -> Option<XdpEgress> {
        if dst != self.target || context.ifindex != self.resolved.egress().ifindex {
            return None;
        }
        let mut egress = self.resolved.egress();
        egress.queue = context.queue;
        Some(egress)
    }

    fn resolve_udp_egress_resolved(
        &self,
        dst: Ipv4Addr,
        context: XdpRouteContext,
    ) -> Option<XdpResolvedEgress> {
        self.resolve_udp_egress(dst, context)
            .map(XdpResolvedEgress::from_egress)
    }

    fn resolve_udp_l2(&self, dst: Ipv4Addr, context: XdpRouteContext) -> Option<ResolvedL2<'_>> {
        if dst != self.target || context.ifindex != self.resolved.egress().ifindex {
            return None;
        }
        Some(ResolvedL2::Borrowed {
            l2_header: self.resolved.l2_header(),
            ip_mtu: context.mtu.min(self.resolved.egress().mtu as usize),
        })
    }
}
```

The important override is `resolve_udp_l2`. `send` calls this method on the
transmit path before writing packet headers. Returning `ResolvedL2::Borrowed`
lets the socket use the prebuilt Ethernet header from `XdpResolvedEgress`
instead of rebuilding the L2 bytes from the route and neighbor tables on every
packet.

## Wiring the Router

`run_static_route_blast` still uses `RouteSnapshot::from_netlink()`, but only on
the cold path. For each worker plan it resolves the target once for that
worker's interface and first queue, converts that result into `XdpResolvedEgress`,
and stores it in `StaticTargetRouter`:

```rust
let queue = plan
    .queue_ids()
    .first()
    .copied()
    .unwrap_or_else(|| QueueId::new(0));
let egress = routes
    .egress_v4_for_interface(*target.ip(), plan.ifindex(), queue)
    .ok_or_else(|| {
        format!(
            "no queue-local netlink route/neighbor entry for {} on ifindex {} queue {}",
            target.ip(),
            plan.ifindex().get(),
            queue.get()
        )
    })?;
let router = StaticTargetRouter {
    target: *target.ip(),
    resolved: XdpResolvedEgress::from_egress(egress),
};
```

The router is then installed when the worker opens its aggregate:

```rust
let mut aggregate = plan
    .open_udp_busy_poll_with_router(local, || router.clone())
    .map_err(|error| error.to_string())?;
```

`open_udp_busy_poll_with_router` is the aggregate-level escape hatch for the
normal XDP UDP routing path. It builds each `BusyPollXdpUdpSocket` with the
caller-supplied `XdpUdpRouter` instead of the default `XdpQueueLocalRouter`.
After that, the worker's packet loop is ordinary UDP batching: allocate TX
buffers, fill payloads, wrap them in `UdpTransmit`, and call `send`.
