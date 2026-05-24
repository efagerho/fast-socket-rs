# Architecture

The architecture has two implemented layers, with room for an optional adapter
layer above IP packet backends.

At the center is `fast-socket-rs`, the backend-agnostic core crate. It defines
the public traits, buffer model, packet metadata, policy types, route and egress
vocabulary. It does not open OS sockets, load XDP programs, allocate DPDK
memory, or depend on backend crates.

Backend crates sit below the core API. They own concrete socket state,
backend-specific buffer pools, file descriptors, rings, UMEM, route snapshots,
and device integration. A backend crate implements the core traits using its own
associated types.

Backend-agnostic adapters can be added above IP packet backends when there is a
clear shared use case, but none are part of the current core crate. Backends
implement `UdpSocket` directly when they want to expose optimized UDP payload
I/O.

The current workspace shape is:

- `fast-socket-rs`: core traits, buffers, policies, and route vocabulary.
- `fast-socket-os-rs`: direct OS-backed UDP implementation.
- `fast-socket-xdp-rs`: AF_XDP IP packet and direct UDP socket implementations,
  plus Linux routing support.
- `fast-socket-xdp-ebpf`: embedded eBPF/XDP program for AF_XDP redirection.
- `benchmarks`: benchmark harnesses and runnable OS/XDP sender/listener tools.

The core principle is one-way dependency flow. Backends depend on the core
crate. The core crate does not depend on backends. Backend crates also avoid
depending on each other unless a future design has a strong reason to share a
backend-specific implementation detail.
