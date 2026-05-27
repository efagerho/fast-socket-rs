# OS Backend

The OS backend lives in `fast-socket-os-rs` and implements `UdpSocket`
directly. It is the portable baseline for the core UDP API.

`OsUdpSocketBuilder` binds a `std::net::UdpSocket`, records logical queue
metadata, configures buffer layouts, and sets the UDP payload MTU.
`OsUdpSocket::from_std` can wrap an existing socket. The socket is put into
nonblocking mode and owned by the worker thread that uses it.

Polling is readiness-based. The backend wraps a cloned UDP socket in
`OsReadinessSource` and exposes it through `ReadinessDriver`.

Buffers come from `OsBufferPool`, a queue-local slab-backed pool. It recycles
fixed-size heap allocations through non-atomic owner-thread state. This avoids
steady allocator churn while preserving the OS UDP receive copy into userspace
memory.

On Linux, send and receive are batched with `sendmmsg` and `recvmmsg`.
Transmit uses packet segments as iovecs and consumes only the messages the
kernel accepts. Receive allocates pool buffers, hands them to `recvmmsg`, then
sets each received packet length before pushing it into the caller's
`RecvBatch`.

On non-Linux platforms, the backend falls back to scalar `send_to` and
`recv_from` loops with the same `UdpSocket` semantics.

The backend reports default UDP capabilities. Completion draining is `Ok(0)`
because transmit ownership is copied into the kernel socket path rather than
retained until a completion queue is drained.

Linux `QueueAffinity::Core(cpu)` maps to `SO_INCOMING_CPU` when available.
Other affinity forms are retained as metadata and do not imply portable socket
option behavior.
