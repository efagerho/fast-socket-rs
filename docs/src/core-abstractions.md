# Core Abstractions

The core crate exposes a small set of abstractions that backend crates implement
without inheriting each other's details.

`UdpSocket` is the transport API. It deals in UDP payload buffers, socket
addresses, UDP metadata, and UDP capabilities such as GSO and GRO. Backends may
implement it directly when that gives the best packet path, as OS and AF_XDP do.

`IpPacketSocket` is the IP packet API. It deals in complete IP datagrams, egress
handles, checksum/offload metadata, and an IP-family policy. This is the main
abstraction for kernel-bypass and forwarding-oriented backends.

`RawDevice` is an optional side API. It reports device identity and state, but
it is not required for packet send or receive.

These traits are generic over concrete associated types:

- receive and transmit buffer pools;
- polling driver;
- receive metadata;
- IP family;
- egress handle.

That structure keeps the hot path concrete. A backend exposes its buffer and
egress types through the trait instead of returning boxed packet objects or
dynamic metadata.

Shared helper types complete the abstraction. `TxSlot` makes ownership transfer
visible during batch send. `RecvBatch` gives the caller receive capacity
control. `SendError` records how many leading packets were accepted before a
send failed.
