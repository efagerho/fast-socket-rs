# IpPacketSocket

`IpPacketSocket` is the IP-packet queue abstraction. It is designed for kernel-bypass
backends, packet forwarding, and adapters that need to construct or inspect IP
datagrams directly.

Every `IpPacketSocket` packet starts at the first byte of an IPv4 or IPv6 header.
The packet is not an Ethernet frame and not only a transport payload.

The associated types define the socket's concrete hot-path representation:

- `RxPool` and `TxPool` own receive and transmit buffers.
- `Family` is a type-level IP-family policy such as `V4Only`, `V6Only`, or
  `Mixed`.
- `Egress` is the backend-specific value consumed by transmit.
- `Driver` is the polling driver.
- `RecvMeta` is the receive metadata type.

`IpPacketTransmit<B, E, F>` contains the complete IP datagram and a fully resolved
egress handle. Optional parsed source and destination hints can be carried for
backends that benefit from them, but the packet bytes remain authoritative.
`IpPacketReceive<B, M>` contains the received IP datagram and metadata.

Like `UdpSocket`, `IpPacketSocket` exposes `drain_tx_completions()` for
zero-copy transmit reclaim and `notify_tx()` for backends that need an explicit
transmit notification. Copy-based or always-ready implementations can keep
those operations as inlined no-ops.

`IpPacketEgress` is intentionally tiny. A backend egress handle only needs to be
copyable and optionally provide a default egress. The core crate provides
`CoreEgress` for simple route or neighbor handles. AF_XDP uses `XdpEgress`,
which includes interface, queue, MAC address, ethertype, VLAN, and MTU facts.

The `IpPacketSocket` queue is owned by one socket value. Current live backend
socket types, including OS UDP and XDP sockets, are intentionally queue-local
and not designed to be freely shared across threads. That gives backends room
to use non-atomic free lists, per-queue rings, and worker-local routing
snapshots.
