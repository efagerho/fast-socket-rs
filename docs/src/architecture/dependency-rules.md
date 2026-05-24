# Dependency Rules

The dependency graph is intentionally simple:

- `fast-socket-rs` depends on no backend crate.
- `fast-socket-os-rs` depends on `fast-socket-rs`.
- `fast-socket-xdp-rs` depends on `fast-socket-rs` and the eBPF support crate.
- `fast-socket-xdp-ebpf` is a backend support crate, not part of the core API.
- Benchmark crates may depend on any implementation they measure.

The core crate must not include conditional backend logic such as "if this is
XDP" or "if this is an OS socket." Backend selection happens by choosing a
concrete type. Behavior specialization happens through associated types,
generic parameters, and policy types.

Backend crates should not redefine core traits. If a backend needs to expose a
new reusable concept, the first question is whether that concept is truly
backend-agnostic. If it is, it belongs in the core crate. If it names a kernel,
driver, NIC, fd, ring, or platform behavior, it belongs in the backend crate.

Adapters follow the same rule. An adapter that can work across several IP
packet backends belongs in `fast-socket-rs`. A direct socket implementation that
assumes AF_XDP descriptor layout or OS socket options belongs in the backend
crate.

These rules protect the zero-overhead goals. A caller using direct OS UDP
should not pull in `IpPacketEgress` resolution. A caller using `XdpIpPacketSocket` should
not pay for UDP header construction. A caller that wants UDP on AF_XDP chooses
the separate `XdpUdpSocket` concrete type.
