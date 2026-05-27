# Workspace Layout

The repository is a Cargo workspace. Each crate has a narrow responsibility, so
backend-specific code stays out of the core API.

`crates/fast-socket-rs`

This is the core crate. It owns:

- `UdpSocket`, `IpPacketSocket`, and `RawDevice`;
- packet transmit and receive item types;
- buffer traits and the heap-backed reference implementation;
- batch containers and send error semantics;
- polling driver traits and type-level IP-family policies;
- route, neighbor, tunnel, and egress resolver traits.

`crates/fast-socket-os-rs`

This crate implements direct OS-backed UDP. It wraps `std::net::UdpSocket`,
puts it in nonblocking mode, uses readiness polling, and provides queue-local
slab-backed packet buffers. Linux uses `sendmmsg` and `recvmmsg`; other
platforms use per-packet `send_to` and `recv_from`.

`crates/fast-socket-xdp-rs`

This crate implements AF_XDP IP packet and UDP backends. It owns XDP socket
configuration, UMEM-backed buffers, rings, egress handles, route snapshots,
netlink integration, route-monitor fanout, and XDP program loading.
`IpPacketSocket` presents complete IP datagrams to the core API. `XdpUdpSocket`
implements `UdpSocket` directly for IPv4 UDP payload I/O. The live backend
still receives and transmits Ethernet frames at the NIC.

`crates/fast-socket-xdp-ebpf`

This crate contains the `no_std` XDP program. The program redirects untagged or
single-VLAN IPv4 and IPv6 Ethernet frames to the AF_XDP socket registered for
the receive queue while no UDP ports are bound. When UDP ports are bound, it
redirects only matching non-fragmented IPv4 UDP packets and leaves unrelated
traffic on the kernel path.

`benchmarks`

This crate contains benchmark and profiling entry points: OS/XDP sender and
listener binaries plus perf helper scripts. It may use multiple backend crates
because it measures end-to-end behavior rather than defining reusable library
abstractions.

`examples`

This crate contains runnable API examples. `blast` demonstrates a generic
`UdpSocket` transmit loop over either OS UDP or AF_XDP UDP. `pong-server`
demonstrates a multi-queue reflection server that keeps backend construction in
`main` and passes each concrete socket to a generic UDP loop.
