# Polling and Readiness

Polling behavior is selected by the socket's concrete `Driver` associated type.
This keeps worker-loop shape generic while avoiding runtime mode branches in the
socket hot path.

`PollDriver` exposes:

- a compile-time `MODE`;
- `wait(timeout)`;
- an optional borrowed `WakeHandle` for reactor integration.

There are two canonical modes.

`ReadinessDriver<S>` wraps a `ReadinessSource`. It is for sockets that can wait
on an external event source such as a file descriptor. On Unix, `WakeHandle`
borrows an fd. On non-Unix platforms it is an opaque borrowed token.

`BusyPollDriver` is for sockets owned by a dedicated worker loop. Its `wait`
operation returns `Spurious`, and its wake handle is `None`. Those methods are
small no-ops that should inline away in optimized code.

Marker traits classify sockets by driver type:

- `ReadinessUdpSocket` and `ReadinessIpPacketSocket`;
- `BusyPollUdpSocket` and `BusyPollIpPacketSocket`.

The OS UDP backend uses readiness mode. It wraps a cloned `std::net::UdpSocket`
as its readiness source and polls it on Unix.

The XDP backend supports first-pass and live busy-poll sockets, plus live
readiness-driven AF_XDP sockets by cloning the AF_XDP fd into an
`XdpReadinessSource`. Both IP packet and direct UDP variants follow this driver
shape through aliases such as `BusyPollXdpIpPacketSocket`, `BusyPollXdpUdpSocket`,
`ReadinessXdpIpPacketSocket`, and `ReadinessXdpUdpSocket`.

A typical worker loop can call `driver.wait`, then receive, send, drain
completions, and notify transmit as needed. The important point is that the
socket type, not a runtime enum, determines which waiting behavior is compiled
into that loop.
