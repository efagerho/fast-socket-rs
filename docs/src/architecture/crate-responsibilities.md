# Crate Responsibilities

The core crate owns shared vocabulary. A type or trait belongs in
`fast-socket-rs` when more than one backend or future adapter can use it
without knowing how that backend is implemented.

Core-owned responsibilities include:

- socket traits and packet item shapes;
- buffer pool, mutable buffer, frozen buffer, and prepend/append error vocabulary;
- metadata types such as `UdpRecvMeta` and `IpPacketRecvMeta`;
- offload flags such as `TxOffload` and UDP capability flags;
- route and egress resolver traits;
- polling driver traits and canonical driver types.

Backend crates own concrete capabilities. A type belongs in a backend crate when
it describes a real operating-system, kernel, NIC, memory, or file-descriptor
detail.

Backend-owned responsibilities include:

- socket builders and backend configuration;
- fd, ring, descriptor, UMEM, or socket state;
- backend-specific buffer pools;
- backend-specific egress handles;
- route snapshot implementations that read platform state;
- device-specific statistics and capability mapping;
- unsafe code required to call kernel or driver APIs.

This split is important for both API clarity and code generation. For example,
`XdpEgress` belongs in `fast-socket-xdp-rs` because it contains MAC addresses,
VLAN state, an interface index, an AF_XDP queue id, and an ethertype. The core
crate only needs to know that it implements `IpPacketEgress`.

Future backend-agnostic adapters should live in the core crate only after a real
shared use case proves their trait bounds and packet-ownership rules. Until
then, direct backend implementations should keep backend-specific parsing,
header construction, and offload behavior in backend crates.
