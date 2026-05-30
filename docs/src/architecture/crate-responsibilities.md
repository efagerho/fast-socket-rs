# Crate Responsibilities

The core crate owns shared vocabulary. A type or trait belongs in
`fast-socket-rs` when multiple backends or adapters can use it without knowing
backend internals.

Core-owned responsibilities include:

- socket traits and packet item shapes;
- buffer pool, mutable buffer, frozen buffer, and prepend/append error vocabulary;
- metadata types such as `UdpRecvMeta` and `IpPacketRecvMeta`;
- offload flags such as `TxOffload` and UDP capability flags;
- route and egress resolver traits;
- polling driver traits and canonical driver types.

Backend crates own concrete capabilities. A type belongs in a backend crate when
it describes an OS, kernel, NIC, memory, or file-descriptor detail.

Backend-owned responsibilities include:

- socket builders and backend configuration;
- fd, ring, descriptor, UMEM, or socket state;
- backend-specific buffer pools;
- backend-specific egress handles;
- route snapshot implementations that read platform state;
- device-specific statistics and capability mapping;
- unsafe code required to call kernel or driver APIs.

This split protects API clarity and code generation. `XdpEgress` and
`XdpResolvedEgress` belong in `fast-socket-xdp-rs` because they contain MAC
addresses, VLAN state, materialized L2 bytes, an interface index, an AF_XDP
queue id, and an ethertype. The core crate only needs to know that `XdpEgress`
implements `IpPacketEgress`.

Backend-agnostic adapters should move into the core crate only after a shared
use case proves their trait bounds and ownership rules. Until then, backends
keep parsing, header construction, and offload behavior in backend crates.
