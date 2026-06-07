# fast-socket-xdp-ebpf

AF_XDP eBPF redirect programs for `fast-socket-xdp-rs`.

The object contains two XDP entrypoints:

- `fast_socket_xdp` uses the `BOUND_PORTS` membership array. It redirects IPv4
  UDP packets whose destination port is enabled in that array and passes
  non-UDP traffic plus unmatched UDP traffic back to Linux.
- `fast_socket_xdp_port_range` uses the `BOUND_PORT_RANGE` start/end array. It
  redirects IPv4 UDP packets whose destination port is inside the inclusive
  range and passes non-UDP traffic plus unmatched UDP traffic back to Linux.

The range program is intended for sockets that need to bind a large contiguous
block of UDP ports without updating one `BOUND_PORTS` slot per port.

The host-side library exposes prebuilt BPF object bytes. The `src/main.rs`
program is built for `bpfel-unknown-none` by `build-ebpf.sh`; ordinary workspace
builds compile a host stub and do not require the BPF toolchain.

Rebuild the object manually after editing the program:

```sh
./build-ebpf.sh
```
