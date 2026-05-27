# Architecture

The architecture has two implemented layers and room for adapters above IP
packet backends.

`fast-socket-rs` is the backend-agnostic core crate. It defines public traits,
the buffer model, packet metadata, policy types, and route/egress vocabulary.
It does not open OS sockets, load XDP programs, allocate DPDK memory, or depend
on backend crates.

Backend crates sit below the core API. They own socket state, buffer pools,
file descriptors, rings, UMEM, route snapshots, and device integration. Each
backend implements the core traits with its own associated types.

Backend-agnostic adapters can sit above IP packet backends when a shared use
case appears. None are in the core crate today. Backends implement `UdpSocket`
directly when they expose optimized UDP payload I/O.

The current workspace shape is:

- `fast-socket-rs`: core traits, buffers, policies, and route vocabulary.
- `fast-socket-os-rs`: direct OS-backed UDP implementation.
- `fast-socket-xdp-rs`: AF_XDP IP packet and direct UDP socket implementations,
  plus Linux routing support.
- `fast-socket-xdp-ebpf`: embedded eBPF/XDP program for AF_XDP redirection.
- `benchmarks`: benchmark harnesses and runnable OS/XDP sender/listener tools.
- `examples`: small API examples, including `blast` and `pong-server`.

The core principle is one-way dependency flow. Backends depend on the core
crate; the core crate does not depend on backends. Backend crates avoid
depending on each other unless a future design needs shared backend-specific
code.
