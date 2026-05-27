# IpPacketSocket

`IpPacketSocket` is the IP-packet queue abstraction for kernel-bypass backends,
packet forwarding, and adapters that construct or inspect IP datagrams.

Every `IpPacketSocket` packet starts at the first byte of an IPv4 or IPv6 header.
The packet is not an Ethernet frame and not only a transport payload.

The associated types define the socket's concrete hot-path representation:

- `RxPool` and `TxPool` own receive and transmit buffers.
- `Family` is a type-level IP-family policy such as `V4Only`, `V6Only`, or
  `Mixed`.
- `Egress` is the backend-specific value consumed by transmit.
- `Driver` is the polling driver.
- `RecvMeta` is the receive metadata type.

`IpPacketTransmit<B, E, F>` contains the complete IP datagram and resolved
egress handle. Backends may carry parsed source and destination hints, but the
packet bytes remain authoritative.
`IpPacketReceive<B, M>` contains the received IP datagram and metadata.

Like `UdpSocket`, `IpPacketSocket` exposes `drain_tx_completions()` for
zero-copy transmit reclaim and `notify_tx()` for explicit transmit notification.
Copy-based or always-ready implementations can make both no-ops.

`IpPacketEgress` is small: a backend egress handle must be copyable and may
provide a default egress. The core crate provides `CoreEgress` for route or
neighbor handles. AF_XDP uses `XdpEgress`, which includes interface, queue, MAC
address, ethertype, VLAN, and MTU facts.

One socket value owns each `IpPacketSocket` queue. Current live backend sockets,
including OS UDP and XDP sockets, are queue-local rather than freely shared
across threads. Backends can use non-atomic free lists, per-queue rings, and
worker-local routing snapshots.
