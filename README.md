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

The `examples` crate has user-facing API examples. Pass `--help` to any binary
for a full flag listing.

### `blast` — single-socket UDP blaster

```bash
cargo run -p fast-socket-examples --bin blast -- \
  --device eth0 \
  --target 192.0.2.10:9000 \
  --mode os
```

Use `--mode xdp` for the AF_XDP-backed socket. The blaster creates one thread
and one `UdpSocket`, then sends 64-byte UDP payloads as fast as the backend
accepts them.

### `pong-server` — multi-queue UDP reflector

```bash
cargo run -p fast-socket-examples --bin pong-server -- \
  --device eth0 \
  --target 192.0.2.20:9000 \
  --mode os
```

Creates one socket per NIC queue and pins worker threads to the queue CPUs. In
OS mode it uses `SO_REUSEPORT` and `SO_INCOMING_CPU`; in XDP mode it binds one
AF_XDP UDP socket per queue. `--target` is the expected peer endpoint: the
server binds the device IP with that port and, in XDP mode, uses the peer IP
for queue-local egress resolution.

### `custom-router` — `XdpUdpRouter` walkthrough

```bash
cargo run -p fast-socket-examples --bin custom-router -- \
  --device eth0 \
  --target 192.0.2.30:9000 \
  --mac de:ad:be:ef:00:01
```

Sends one 64-byte UDP payload per second through an `XdpUdpRouter` whose route
table is constant (default ifindex) and whose ARP table returns the configured
MAC for every destination. The smallest end-to-end example of bypassing the
default `XdpQueueLocalRouter`.

## Benchmarks

The `benchmarks` crate has runnable sender/listener tools used by the design
book's performance chapter. Each binary is a clap CLI; pass `--help` for the
full flag listing.

### Listeners

| Binary | What it does |
|--------|--------------|
| `os-listener` | `count` or `pong` mode against OS UDP sockets with optional `SO_REUSEPORT` / `SO_INCOMING_CPU` fan-out. |
| `xdp-listener` | Same shape as `os-listener` but on AF_XDP IP packet sockets, one socket per queue, coalesced onto the CPU each queue is pinned to. |

```bash
cargo run -p fast-socket-benchmarks --bin os-listener -- \
  pong --bind 192.0.2.20:9000 --reuse-port

cargo run -p fast-socket-benchmarks --bin xdp-listener -- \
  pong --iface eth0 --bind 192.0.2.20:9000 --duration-ms 30000
```

### Senders

| Binary | What it does |
|--------|--------------|
| `os-sender` | Single- or multi-thread `blast` against an OS UDP destination. |
| `xdp-sender` | AF_XDP UDP blast, one socket or one per queue, coalesced onto the CPU each queue is pinned to. Per-queue source-port allocation when `--all-queues` is used. |

```bash
cargo run -p fast-socket-benchmarks --bin os-sender -- \
  blast --dest 192.0.2.10:9000 --threads 4 --duration-ms 10000

cargo run -p fast-socket-benchmarks --bin xdp-sender -- \
  blast --iface eth0 --all-queues \
  --local 192.0.2.20:60000 --dest 192.0.2.10:9000 \
  --duration-ms 10000
```

`--count N` stops after sending N packets; `--duration-ms N` stops after N
milliseconds. Either or both can be set.

## Documentation

The design book lives in `docs`.

```bash
mdbook serve docs
```

Start with `docs/src/overview.md` for the API shape, then read the architecture,
packet model, buffer, and backend chapters for the invariants behind the
implementations.
