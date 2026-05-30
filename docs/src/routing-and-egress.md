# Routing and Egress

IP packet transmit requires a backend egress handle. The core crate defines the
resolution vocabulary; backend crates decide what a resolved egress contains.

The core route traits are:

- `RouteTable<F>` maps a destination IP to an outgoing interface and next hop.
- `NeighborTable<F>` maps a next-hop IP to a link-layer address.
- `TunnelTable<Inner, Outer>` maps an inner destination to an optional tunnel
  target.
- `EgressResolver<F, E>` maps a destination IP directly to the backend egress
  handle consumed by `IpPacketSocket::send`.

`CoreEgress` is a shared handle for simple cases: default egress, route id, or
neighbor id. Backends with richer needs define their own egress type and
implement `IpPacketEgress`.

AF_XDP uses `XdpEgress`. It contains the outgoing interface, queue, destination
MAC, source MAC, ethertype, optional VLAN id, and effective IP-layer MTU. That
is enough to prepend the Ethernet header before placing a descriptor on the TX
ring.

AF_XDP UDP transmit asks routers for `XdpResolvedEgress`, which wraps the
`XdpEgress` plus materialized Ethernet or VLAN header bytes. Routers that only
implement the older `resolve_udp_egress` method continue to work: the default
adapter builds the resolved form on demand. Routers with stable destinations can
override `resolve_udp_egress_resolved` and return prebuilt L2 bytes directly.

`XdpUdpSocket` does not store a pre-resolved destination egress. It is generic
over an `XdpUdpRouter`, and the default `XdpQueueLocalRouter` resolves each UDP
destination through its queue-local route snapshot.

The XDP route implementation uses immutable snapshots. `RouteSnapshot` stores
IPv4 routes, neighbors, and interface facts, resolves longest-prefix routes,
and builds `XdpEgress`. For IPv4 gateway routes, the snapshot also precomputes
the resolved AF_XDP egress, including the L2 header, whenever routes,
neighbors, or interface facts change. Direct routes still resolve dynamically
because the next-hop address is the destination itself. `XdpLocalRoutes` keeps a
queue-local snapshot and adopts pending updates outside the packet path.

Route monitoring follows the same cold-path pattern. One `XdpRouteMonitor` fans
snapshots out to many queue owners. Each published snapshot already contains
its gateway-route precomputation; with netlink monitoring, the monitor thread
pays that cost before publishing. Each queue applies updates through its own
handle, then resolves egress from local immutable memory in the hot path.

The design leaves room for tunnels without making every `IpPacketTransmit`
carry tunnel branches. Tunnel lookup is shared vocabulary, but a non-tunnel
resolver can optimize to no residual tunnel code.
