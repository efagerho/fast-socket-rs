# OS Backend

The OS backend lives in `fast-socket-os-rs` and implements `UdpSocket`
directly. It is the portable baseline backend and the simplest way to use the
core UDP API.

`OsUdpSocketBuilder` binds a `std::net::UdpSocket`, records logical queue
metadata, configures buffer layouts, and sets the effective UDP payload MTU.
`OsUdpSocket::from_std` can also wrap an already-created socket. The socket is
put into nonblocking mode and is intentionally owned by the worker thread that
uses it.

Polling is readiness-based. The backend wraps a cloned UDP socket in
`OsReadinessSource` and exposes it through `ReadinessDriver`.

Buffers come from `OsBufferPool`, a queue-local slab-backed pool. The pool
recycles fixed-size heap allocations through non-atomic owner-thread state. This
avoids steady allocator churn while still reflecting the fact that ordinary OS
UDP receive copies payload bytes into userspace memory.

On Linux, send and receive are batched with `sendmmsg` and `recvmmsg`.
Transmit uses packet segments as iovecs and consumes only the messages the
kernel accepts. Receive allocates pool buffers, hands them to `recvmmsg`, then
sets each received packet length before pushing it into the caller's
`RecvBatch`.

On non-Linux platforms, the backend falls back to scalar `send_to` and
`recv_from` loops while preserving the same `UdpSocket` semantics.

The backend currently reports default UDP capabilities. Completion draining is
`Ok(0)` because transmit ownership is copied into the kernel socket path rather
than retained until a separate completion queue is drained.

Linux `QueueAffinity::Core(cpu)` maps to `SO_INCOMING_CPU` when available.
Other affinity forms are retained as metadata and do not imply portable socket
option behavior.
