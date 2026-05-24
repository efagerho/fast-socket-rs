# Overview

Fast Socket is a Rust workspace for high-performance packet I/O. Its central
idea is to keep the public socket shape stable while allowing very different
backends to implement it: ordinary operating-system UDP sockets today, AF_XDP
IP packet and direct UDP queues today, and DPDK-style backends later.

The workspace is organized around two steady-state packet interfaces:

- `UdpSocket` is the transport-facing API. It sends and receives UDP payloads
  with socket-address metadata.
- `IpPacketSocket` is the IP-packet API. It sends and receives complete IPv4 or
  IPv6 datagrams, starting at the IP header.

A third interface, `RawDevice`, is deliberately a side API. It exposes device
identity, queue affinity, NUMA hints, capabilities, statistics, and MTU refresh
without becoming part of every packet operation.

The design favors static structure over runtime switching. Backends choose
their buffer pools, metadata, egress handles, polling drivers, and IP-family
policy as associated types. Direct backend sockets such as `OsUdpSocket` and
`XdpUdpSocket` compose those concrete types with backend-local policy code, so
unused features can disappear after monomorphization.

This book documents the intended design constraints as much as the current API.
The most important invariants are:

- the core crate is backend-agnostic;
- backend crates implement core traits instead of redefining them;
- batch operations consume packet ownership explicitly;
- buffer layout, headroom, tailroom, and completion semantics are visible;
- optional capabilities do not add cost to unrelated hot paths.

Read the architecture chapters first for crate boundaries, then the core
abstraction and packet model chapters for the API invariants. The backend and
benchmarking chapters explain how those invariants are meant to survive real
OS, AF_XDP, and future DPDK implementations.
