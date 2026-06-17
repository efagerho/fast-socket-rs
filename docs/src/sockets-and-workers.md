# Sockets and Workers

The direct API is built around one owner thread per live socket or aggregate.
Create the socket on the thread that will drive it, keep reusable batch storage
next to that socket, and make progress by calling `recv`, `send`,
`drain_tx_completions`, and `notify_tx` from that same thread.

Packet buffers may move to other threads, but the socket and its pools must
outlive every buffer they hand out. That rule is what lets the hot path avoid
reference-counting each packet.

## OS UDP Worker

The OS backend opens one wait-driven UDP socket. A typical worker configures the
socket, applies the socket's affinity hint when one is configured, and then runs
the application step function until shutdown:

```rust,ignore
use std::net::{Ipv4Addr, SocketAddrV4};
use std::thread;
use std::time::Duration;

use fast_socket_os_rs::OsUdpSocketBuilder;
use fast_socket_rs::{
    BufferLayout, QueueAffinity, UdpSocket, pin_current_thread_to_socket,
};

let bind = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 9000);
let handle = thread::spawn(move || -> Result<(), Box<dyn std::error::Error>> {
    let mut socket = OsUdpSocketBuilder::new(bind.into())
        .bind_to_device("eth0")
        .queue_affinity(QueueAffinity::Core(3))
        .buffer_layout(BufferLayout::for_payload(2048))
        .max_batch(64)
        .pool_max_buffers(256)
        .mtu(1472)
        .bind()?;

    let _pin = pin_current_thread_to_socket(&socket)?;

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

`queue_affinity` is only a hint on the socket. The worker still calls
`pin_current_thread_to_socket` on the thread that owns the socket. When the
socket reports `QueueAffinity::Any`, the helper returns `PinOutcome::NoHint`.

## XDP Worker Plans

AF_XDP sockets are usually opened through `XdpFactoryBuilder`. The factory runs
the discovery and program setup once, then returns `XdpWorkerPlan` values that
can be moved to worker threads. Each plan opens one aggregate over the queues
assigned to that worker:

```rust,ignore
use std::net::SocketAddrV4;
use std::thread;

use fast_socket_xdp_rs::{
    InterfaceSelector, PortFilter, RouteSnapshot, XdpFactoryBuilder,
};

let local: SocketAddrV4 = "192.0.2.10:9000".parse()?;
let factory = XdpFactoryBuilder::new(InterfaceSelector::Name("eth0".to_string()))?
    .threads(4)
    .port_filter(PortFilter::UdpPorts(vec![local.port()]))
    .route_snapshot(RouteSnapshot::from_netlink()?)
    .build()?;

let mut handles = Vec::new();
for plan in factory.into_worker_plans() {
    handles.push(thread::spawn(move || -> Result<(), Box<dyn std::error::Error>> {
        let mut aggregate = plan.open_udp_busy_poll(local)?;
        while should_continue() {
            let mut progressed = 0usize;
            for socket in aggregate.members_mut() {
                progressed += app_step(socket)?;
            }
            if progressed == 0 {
                aggregate.drain_tx_completions()?;
                std::thread::yield_now();
            }
        }
        Ok(())
    }));
}
```

`open_udp_busy_poll` and `open_udp_wait_driven` pin the current thread to
`plan.cpu()` before opening the aggregate so UMEM, rings, and scratch state are
placed for that worker. Use `open_udp_busy_poll_unpinned` or
`open_udp_wait_driven_unpinned` when the application has already pinned the
thread, is using a custom scheduler, or needs to defer affinity to the runtime.

## Wait-Driven Actors

Wait-driven sockets can also be handed to the Tokio actor layer. The actor owns
the socket and drives its `PollDriver`; application tasks use `AsyncUdpRx` and
`AsyncUdpHandle` rather than touching the socket directly:

```rust,ignore
use fast_socket_async_rs::{ActorConfig, spawn_udp_actor_local};
use fast_socket_xdp_rs::{InterfaceSelector, RouteSnapshot, XdpFactoryBuilder};

let factory = XdpFactoryBuilder::new(InterfaceSelector::Name("eth0".to_string()))?
    .threads(2)
    .route_snapshot(RouteSnapshot::from_netlink()?)
    .build()?;

let mut actors = Vec::new();
for plan in factory.into_worker_plans() {
    let aggregate = plan.open_udp_wait_driven_unpinned(local)?;
    for socket in aggregate.into_members() {
        actors.push(spawn_udp_actor_local(
            socket,
            ActorConfig {
                recv_batch_size: 64,
                ..ActorConfig::default()
            },
        )?);
    }
}
```

The unpinned open path is useful with single-threaded Tokio executors and
`LocalSet`: the runtime decides where the task runs, while the actor still owns
the socket and waits through the socket's driver.

## Escape Hatches

The default constructors choose conservative settings, but the public API leaves
the important deployment choices visible:

- OS sockets: `OsUdpSocketBuilder` can override interface binding, interface
  index metadata, queue id, queue affinity, RX and TX buffer layouts, MTU,
  `SO_REUSEPORT`, syscall batch size, and RX/TX pool caps. Call
  `OsUdpSocket::from_std` when a platform socket must be preconfigured outside
  the builder.
- Generic pinning: every `UdpSocket` and `IpPacketSocket` may report
  `worker_affinity`. Use `pin_current_thread_to_socket`,
  `pin_current_thread_to_ip_packet_socket`, `pin_current_thread_to_affinity`, or
  `pin_current_thread_to_cpu` when the application owns thread placement.
- XDP factory setup: `XdpFactoryBuilder` can override queue claims, thread
  count, UDP port filters, UMEM frame count, hugepage preference, MTU, ring
  sizes, AF_XDP bind mode, XDP attach mode, per-queue buffer configuration,
  route snapshot, and core assignment.
- XDP worker placement: `XdpWorkerPlan::new` bypasses factory partitioning when
  an application wants to supply queue grouping, CPU choice, NUMA hint, and
  socket config directly.
- XDP opening: pinned `open_udp_*` methods are the normal path; unpinned
  `open_udp_*_unpinned` methods let callers manage affinity themselves.
- XDP routing: `open_udp_busy_poll_with_router`,
  `open_udp_wait_driven_with_router`, and `XdpUdpSocketBuilder::router` replace
  the default queue-local router. Custom `XdpUdpRouter` implementations can
  return cached egress state or override `resolve_udp_l2` to borrow prebuilt L2
  headers on the transmit path.
- XDP single-queue builders: `XdpIpPacketSocketBuilder` and
  `XdpUdpSocketBuilder` expose lower-level socket construction, including custom
  program bytes, attached program reuse, UDP parser port sets, route snapshots,
  NUMA hints, ring sizes, buffers, MTU, frame count, and attach mode.

The runnable binaries under `examples/` use these same pieces. Start with
`examples/src/common.rs` for the common OS loop, XDP aggregate loop, and Tokio
actor wiring, then use the per-binary docs for packet-specific logic.
