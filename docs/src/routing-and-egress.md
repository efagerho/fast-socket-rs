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

`XdpUdpSocket` does not store a pre-resolved destination egress. It is generic
over an `XdpUdpEgressResolver`, and the default `XdpQueueLocalUdpResolver`
resolves each UDP destination through the wrapped IP socket's queue-local route
snapshot.

The XDP route implementation uses immutable snapshots. `RouteSnapshot` stores
IPv4 routes, neighbors, and interface facts, resolves longest-prefix routes,
and builds `XdpEgress`. `XdpLocalRoutes` keeps a queue-local snapshot and adopts
pending updates outside the packet path.

Route monitoring follows the same cold-path pattern. One `XdpRouteMonitor` fans
snapshots out to many queue owners. Each queue applies updates through its own
handle, then resolves egress from local immutable memory in the hot path.

The design leaves room for tunnels without making every `IpPacketTransmit`
carry tunnel branches. Tunnel lookup is shared vocabulary, but a non-tunnel
resolver can optimize to no residual tunnel code.
