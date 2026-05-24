# Core Abstractions

The core crate exposes a small set of abstractions that backend crates can
implement without inheriting each other's details.

`UdpSocket` is the high-level transport API. It deals in UDP payload buffers,
socket addresses, UDP metadata, and UDP-specific capabilities such as GSO and
GRO. Backends may implement it directly when that gives the best packet path,
as the OS and AF_XDP backends do.

`IpPacketSocket` is the IP packet API. It deals in complete IP datagrams, egress
handles, checksum/offload metadata, and an IP-family policy. This is the main
abstraction for kernel-bypass and forwarding-oriented backends.

`RawDevice` is an optional side API. It reports device identity and state, but
it is not required for packet send or receive.

These traits are deliberately generic over concrete associated types:

- receive and transmit buffer pools;
- polling driver;
- receive metadata;
- IP family;
- egress handle.

That structure is the mechanism that keeps the hot path concrete. A backend
does not return boxed packet objects or dynamic metadata by default. It exposes
its own concrete buffer and egress types through the trait, and generic callers
compile against those types directly.

The shared helper types are part of the abstraction as well. `TxSlot` makes
ownership transfer visible during batch send. `RecvBatch` gives the caller
control over receive capacity. `SendError` records how many leading packets were
accepted before a send failed.
