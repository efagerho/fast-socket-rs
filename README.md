# Fast Socket

Fast Socket is a Rust workspace for high-performance packet I/O. It keeps one
socket API shape across OS UDP sockets, AF_XDP IP packet queues, AF_XDP UDP
queues, and future DPDK-style backends.

The core traits are:

- `UdpSocket`: sends and receives UDP payloads with socket-address metadata.
- `IpPacketSocket`: sends and receives complete IPv4 or IPv6 datagrams starting
  at the IP header.
- `RawDevice`: exposes device identity, queue affinity, NUMA hints,
  capabilities, statistics, and MTU refresh as a side API.

The design favors static dispatch and backend-owned associated types so unused
features can optimize away from hot packet paths.

## Workspace

- `crates/fast-socket-rs`: core traits, buffers, batches, polling policies, and
  route vocabulary.
- `crates/fast-socket-os-rs`: direct OS-backed UDP implementation.
- `crates/fast-socket-xdp-rs`: AF_XDP IP packet and direct UDP implementations,
  plus Linux route and interface helpers.
- `crates/fast-socket-xdp-ebpf`: embedded XDP redirection program.
- `benchmarks`: runnable sender/listener tools and profiling scripts.
- `examples`: small API examples, including the `blast` packet blaster.
- `docs`: mdBook design documentation.

## Build And Test

```bash
cargo check --workspace
cargo test --workspace
```

AF_XDP live paths require Linux, suitable privileges, NIC queue setup, and an
attachable XDP program. Unprivileged tests use in-memory first-pass paths where
possible.

## Examples

Run the generic UDP packet blaster with either OS sockets or AF_XDP:

```bash
cargo run -p fast-socket-examples --bin blast -- \
  --device eth0 \
  --target 192.0.2.10:9000 \
  --mode os
```

Use `--mode xdp` for the AF_XDP-backed socket. The blaster creates one thread
and one `UdpSocket`, then sends 64-byte UDP payloads as fast as the backend
accepts them.

The examples crate also includes a multi-queue reflection server:

```bash
cargo run -p fast-socket-examples --bin pong-server -- \
  --device eth0 \
  --target 192.0.2.20:9000 \
  --mode os
```

`pong-server` creates one socket per NIC queue and pins worker threads to the
queue CPUs. In OS mode it uses `SO_REUSEPORT` and `SO_INCOMING_CPU`; in XDP mode
it binds one AF_XDP UDP socket per queue. `--target` names the expected peer
endpoint; the server binds the device IP with that port and uses the peer IP for
XDP egress resolution.

## Documentation

The design book lives in `docs`.

```bash
mdbook serve docs
```

Start with `docs/src/overview.md` for the API shape, then read the architecture,
packet model, buffer, and backend chapters for the invariants behind the
implementations.
