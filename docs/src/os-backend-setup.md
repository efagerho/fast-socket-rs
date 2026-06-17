# OS Backend Setup

`OsUdpSocketBuilder` is the normal entry point for OS-backed UDP sockets. It
binds one nonblocking UDP socket, configures the packet pools around that
socket, and records the worker metadata exposed through the core `UdpSocket`
trait.

The builder controls three deployment choices:

- which local address and device the socket binds to, through `new` and
  `bind_to_device`;
- whether multiple workers can bind the same UDP address, through
  `reuse_port`;
- which CPU the socket prefers, through `queue_affinity`.

OS sockets do not have an XDP-style factory because queue discovery and packet
steering stay in the kernel. Create one builder per worker socket, then move the
opened socket to the thread or runtime task that will own it.

## One Worker Socket

Use this when one thread owns one UDP socket. The socket reports
`QueueAffinity::Any` unless a more specific hint is configured.

```rust,ignore
use std::net::{Ipv4Addr, SocketAddrV4};
use std::thread;
use std::time::Duration;

use fast_socket_os_rs::OsUdpSocketBuilder;
use fast_socket_rs::{BufferLayout, UdpSocket};

let bind = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 9000);

thread::spawn(move || -> Result<(), Box<dyn std::error::Error>> {
    let mut socket = OsUdpSocketBuilder::new(bind.into())
        .bind_to_device("eth0")
        .buffer_layout(BufferLayout::for_payload(2048))
        .max_batch(64)
        .pool_max_buffers(256)
        .mtu(1472)
        .bind()?;

    while should_continue() {
        let count = app_step(&mut socket)?;
        if count == 0 {
            socket.drain_tx_completions()?;
            socket.driver_mut().wait(Some(Duration::from_micros(50)))?;
        }
    }
    Ok(())
});
```

`max_batch` sizes the `recvmmsg` and `sendmmsg` chunks plus their syscall
scratch storage. If no pool cap is set, each packet pool defaults to a small
multiple of `max_batch`; use `pool_max_buffers`, `rx_pool_max_buffers`, or
`tx_pool_max_buffers` when retained packet memory should be capped explicitly.

## Per-Core Reuseport Workers

Use this when several owner threads should share one local UDP port. Each worker
opens its own socket with `reuse_port(true)`, and the kernel distributes packets
across the compatible sockets in the reuseport group.

```rust,ignore
use std::net::{Ipv4Addr, SocketAddrV4};
use std::thread;

use fast_socket_os_rs::OsUdpSocketBuilder;
use fast_socket_rs::{QueueAffinity, QueueId, UdpSocket, pin_current_thread_to_socket};

let bind = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 9000);
let cpus = [2_u32, 3, 4, 5];

for (index, cpu) in cpus.into_iter().enumerate() {
    thread::spawn(move || -> Result<(), Box<dyn std::error::Error>> {
        let mut socket = OsUdpSocketBuilder::new(bind.into())
            .bind_to_device("eth0")
            .reuse_port(true)
            .queue_id(QueueId::new(index as u32))
            .queue_affinity(QueueAffinity::Core(cpu))
            .max_batch(64)
            .bind()?;

        let _pin = pin_current_thread_to_socket(&socket)?;
        run_worker(&mut socket)?;
        Ok(())
    });
}
```

All sockets in the group must use compatible bind settings. `reuse_port(true)`
sets `SO_REUSEPORT` before `bind`; it does not choose the kernel's reuseport
hashing policy or install a custom reuseport eBPF program.

## Incoming CPU Steering

Use `queue_affinity(QueueAffinity::Core(cpu))` when a socket should prefer a
specific CPU. On Linux this maps to `SO_INCOMING_CPU`, and the same hint is
returned by `socket.worker_affinity()` so the application can pin the owner
thread.

```rust,ignore
use std::net::{Ipv4Addr, SocketAddrV4};

use fast_socket_os_rs::OsUdpSocketBuilder;
use fast_socket_rs::{QueueAffinity, QueueId, UdpSocket, pin_current_thread_to_socket};

let mut socket = OsUdpSocketBuilder::new(
    SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 9000).into(),
)
.bind_to_device("eth0")
.queue_id(QueueId::new(7))
.queue_affinity(QueueAffinity::Core(7))
.bind()?;

let _pin = pin_current_thread_to_socket(&socket)?;
run_worker(&mut socket)?;
```

`QueueAffinity::Core` is the only affinity form that changes an OS socket
option today. `QueueAffinity::Any` means no CPU preference, and
`QueueAffinity::Mask` is retained as metadata for callers that want to interpret
the mask themselves.

## Device-Scoped Socket

Use `bind_to_device` when the socket should only receive traffic from one
network device. On Linux this sets `SO_BINDTODEVICE` before binding and records
the interface index in the socket metadata.

```rust,ignore
use std::net::{Ipv4Addr, SocketAddrV4};

use fast_socket_os_rs::OsUdpSocketBuilder;
use fast_socket_rs::{BufferLayout, IfIndex};

let mut socket = OsUdpSocketBuilder::new(
    SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 9000).into(),
)
.bind_to_device("eth0")
.if_index(IfIndex::new(2))
.buffer_layout(BufferLayout::for_payload(4096))
.mtu(1472)
.bind()?;

run_worker(&mut socket)?;
```

The explicit `if_index` is optional when `bind_to_device` can resolve the device
name. Use it when the application already knows the interface index or wants the
metadata to stay independent from platform name resolution.

## Runtime Owns Thread Placement

OS UDP sockets are wait-driven, so they can be handed directly to the Tokio
actor layer. The actor owns the socket and waits through its `PollDriver`; async
tasks use the returned handles instead of touching the socket directly.

```rust,ignore
use std::net::{Ipv4Addr, SocketAddrV4};

use fast_socket_async_rs::{ActorConfig, spawn_udp_actor_local};
use fast_socket_os_rs::OsUdpSocketBuilder;

let socket = OsUdpSocketBuilder::new(
    SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 9000).into(),
)
.bind_to_device("eth0")
.max_batch(64)
.bind()?;

let actor = spawn_udp_actor_local(
    socket,
    ActorConfig {
        recv_batch_size: 64,
        ..ActorConfig::default()
    },
)?;
```

Use `reuse_port(true)` on each actor socket when several local actor tasks
should share the same UDP port. Leave `queue_affinity` as `Any` when the runtime
is responsible for placement.

## Preconfigured Socket

Use `OsUdpSocket::from_std` when the application needs socket options that are
not exposed by `OsUdpSocketBuilder`. Configure and bind the `std::net::UdpSocket`
first, then wrap it with matching `OsUdpSocketConfig` metadata.

```rust,ignore
use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};

use fast_socket_os_rs::{OsUdpSocket, OsUdpSocketConfig};
use fast_socket_rs::{QueueAffinity, QueueId};

let std_socket = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 9000))?;
std_socket.set_nonblocking(true)?;

let mut socket = OsUdpSocket::from_std(
    std_socket,
    OsUdpSocketConfig {
        queue_id: QueueId::new(0),
        queue_affinity: QueueAffinity::Any,
        max_batch: 64,
        ..OsUdpSocketConfig::default()
    },
)?;

run_worker(&mut socket)?;
```

The wrapper config is the source of fast-socket metadata such as queue id,
affinity hint, buffer layout, MTU, batch size, and pool caps. Platform socket
options must already be installed on the `std::net::UdpSocket` before wrapping.
