# Routing and Egress

IP packet transmit requires a backend egress handle. The core crate defines the
vocabulary for resolving one, while backend crates decide what a fully resolved
egress value contains.

The core route traits are:

- `RouteTable<F>` maps a destination IP to an outgoing interface and next hop.
- `NeighborTable<F>` maps a next-hop IP to a link-layer address.
- `TunnelTable<Inner, Outer>` maps an inner destination to an optional tunnel
  target.
- `EgressResolver<F, E>` maps a destination IP directly to the backend egress
  handle consumed by `IpPacketSocket::send`.

`CoreEgress` is a small shared handle for simple cases: default egress, route
id, or neighbor id. Backends with richer needs should define their own egress
type and implement `IpPacketEgress`.

AF_XDP uses `XdpEgress`. It contains the outgoing interface, queue, destination
MAC, source MAC, ethertype, optional VLAN id, and effective IP-layer MTU. This
is enough for the backend to prepend the Ethernet header immediately before
placing a descriptor on the TX ring.

The XDP route implementation uses immutable snapshots. `RouteSnapshot` stores
IPv4 routes, neighbors, and interface facts, resolves longest-prefix routes,
and can build an `XdpEgress`. `XdpLocalRoutes` keeps a queue-local snapshot and
adopts pending updates outside the packet path.

Route monitoring follows the same cold-path pattern. One `XdpRouteMonitor` can
fan snapshots out to many queue owners. Each queue applies updates through its
own handle, then resolves egress from local immutable memory in the hot path.

The design leaves room for tunnels without requiring every `IpPacketTransmit` to
carry tunnel branches. Tunnel lookup is part of the shared vocabulary, but a
non-tunnel resolver can be a concrete type with no residual tunnel code after
optimization.
