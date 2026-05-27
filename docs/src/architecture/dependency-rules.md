# Dependency Rules

The dependency graph is simple:

- `fast-socket-rs` depends on no backend crate.
- `fast-socket-os-rs` depends on `fast-socket-rs`.
- `fast-socket-xdp-rs` depends on `fast-socket-rs` and the eBPF support crate.
- `fast-socket-xdp-ebpf` is a backend support crate, not part of the core API.
- Benchmark crates may depend on any implementation they measure.

The core crate must not include conditional backend logic such as "if this is
XDP" or "if this is an OS socket." Backend selection happens by choosing a
concrete type. Specialization happens through associated types, generic
parameters, and policy types.

Backend crates should not redefine core traits. If a backend needs a reusable
concept, first decide whether it is backend-agnostic. If yes, it belongs in the
core crate. If it names a kernel, driver, NIC, fd, ring, or platform behavior,
it belongs in the backend crate.

Adapters follow the same rule. An adapter that can work across several IP
packet backends belongs in `fast-socket-rs`. A direct socket implementation that
assumes AF_XDP descriptor layout or OS socket options belongs in the backend
crate.

These rules protect the zero-overhead goals. Direct OS UDP should not pull in
`IpPacketEgress` resolution. `XdpIpPacketSocket` should not pay for UDP header
construction. UDP on AF_XDP uses the separate `XdpUdpSocket` concrete type.
