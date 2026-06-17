# fast-socket-rs

`fast-socket-rs` is an experimental Rust workspace for explicit, batch-oriented
UDP packet processing. The core API keeps packet memory ownership, socket
driving, transmit back-pressure, and completion draining visible to the
application.

The workspace currently focuses on two application styles:

- drive `UdpSocket` implementations directly for tight worker loops;
- use `fast-socket-async-rs` when a Tokio application wants an actor task to own
  a wait-driven socket while lending real packet buffers to async tasks.

## Workspace

- `crates/fast-socket-rs`: core packet buffers, pools, batches, polling traits,
  UDP socket traits, IP packet socket traits, and routing contracts.
- `crates/fast-socket-os-rs`: wait-driven UDP sockets built on nonblocking OS
  sockets.
- `crates/fast-socket-xdp-rs`: Linux AF_XDP UDP and IP packet backends.
- `crates/fast-socket-async-rs`: Tokio actor integration for wait-driven UDP
  sockets.
- `crates/fast-socket-xdp-ebpf`: eBPF support used by the XDP backend.
- `crates/poptrie`: longest-prefix-match route tables.
- `examples`: runnable server and packet-generation examples.
- `docs`: the mdBook source for the design and API guide.

## Build And Check

```sh
cargo check --workspace
cargo fmt --check
mdbook test docs
mdbook build docs
```

The generated book is written to `docs/book`. The source table of contents is
in `docs/src/SUMMARY.md`.

## Examples

List the example binaries:

```sh
cargo run -p fast-socket-examples --bin udp-echo -- --help
cargo run -p fast-socket-examples --bin udp-pong -- --help
cargo run -p fast-socket-examples --bin udp-discard -- --help
cargo run -p fast-socket-examples --bin udp-proxy -- --help
cargo run -p fast-socket-examples --bin udp-tokio-echo -- --help
cargo run -p fast-socket-examples --bin udp-tokio-pong -- --help
cargo run -p fast-socket-examples --bin udp-tokio-discard -- --help
cargo run -p fast-socket-examples --bin udp-tokio-proxy -- --help
cargo run -p fast-socket-examples --bin endpoint-blast -- --help
```

The AF_XDP examples require Linux, an appropriate NIC/interface setup, and the
permissions needed to create XDP sockets and load eBPF programs.

## Documentation

Start with:

- `docs/src/core-design.md` for the ownership and batching model;
- `docs/src/core-traits.md` for the current public trait surface;
- `docs/src/xdp-backend-setup.md` for common AF_XDP factory deployment shapes;
- `docs/src/os-backend-setup.md` for common OS socket deployment shapes;
- `docs/src/examples/index.md` for the runnable example binaries.

## License

This project is licensed under the Apache License, Version 2.0. See `LICENSE`
for the full license text.
