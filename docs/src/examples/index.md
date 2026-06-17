# Example Binaries

The `examples` package contains small binaries that exercise the core UDP
socket traits through OS sockets, AF_XDP sockets, and the Tokio actor adapter.
Run any binary with:

```sh
cargo run -p fast-socket-examples --bin <name> -- <args>
```

Most examples share these flags:

- `--backend os|xdp`, selecting the direct OS backend or AF_XDP backend;
- `--device <ifname>`, used for OS bind-to-device and XDP interface selection;
- `--bind <ipv4:port>`, the local UDP address;
- `--batch-size <n>`, default `64`;
- `--threads <n>`, default `1`, used by XDP factory-based examples;
- `--payload-capacity <bytes>`, default `2048`, for receive and echo/proxy
  buffer layout.

The pages below are organized by program rather than by binary. Programs with a
Tokio variant document both the direct `UdpSocket` loop and the
`fast-socket-async-rs` actor loop in the same chapter. XDP examples need the
usual AF_XDP privileges and route or neighbor state for the selected interface.

Each page includes the packet-processing loop or callback, then explains whether
transmit buffers are allocated or receive buffers are forwarded and how sent
packets are submitted.

| Program | Direct socket binary | Tokio actor binary | Purpose |
| --- | --- | --- | --- |
| `udp-discard` | `udp-discard` | `udp-tokio-discard` | Receive UDP packets and drop them. |
| `udp-echo` | `udp-echo` | `udp-tokio-echo` | Echo received UDP payloads back to each sender. |
| `udp-pong` | `udp-pong` | `udp-tokio-pong` | Reply to each packet with a generated fixed payload. |
| `udp-proxy` | `udp-proxy` | `udp-tokio-proxy` | Forward packets between clients and one upstream endpoint. |
| `udp-xdp-static-route-blast` | `udp-xdp-static-route-blast` | None | XDP-only UDP packet generator for one routed target. |
