# Overview

Fast Socket is a Rust workspace for high-performance packet I/O. It keeps one
public socket shape across different backends: OS UDP sockets, AF_XDP IP packet
and UDP queues, and future DPDK-style backends.

The workspace is organized around two steady-state packet interfaces:

- `UdpSocket` is the transport-facing API. It sends and receives UDP payloads
  with socket-address metadata.
- `IpPacketSocket` is the IP-packet API. It sends and receives complete IPv4 or
  IPv6 datagrams, starting at the IP header.

`RawDevice` is a side API. It exposes device identity, queue affinity, NUMA
hints, capabilities, statistics, and MTU refresh without joining every packet
operation.

The design favors static structure over runtime switching. Backends choose
buffer pools, metadata, egress handles, polling drivers, and IP-family policy
as associated types. Direct backend sockets such as `OsUdpSocket` and
`XdpUdpSocket` compose those types with backend-local policy code, so unused
features can disappear after monomorphization.

This book documents the design constraints and the current API. Key invariants:

- the core crate is backend-agnostic;
- backend crates implement core traits instead of redefining them;
- batch operations consume packet ownership explicitly;
- buffer layout, headroom, tailroom, and completion semantics are visible;
- optional capabilities do not add cost to unrelated hot paths.

Read the architecture chapters first for crate boundaries, then the core
abstraction and packet model chapters for API invariants. The backend and
benchmarking chapters apply those invariants to OS, AF_XDP, and future DPDK
implementations.
