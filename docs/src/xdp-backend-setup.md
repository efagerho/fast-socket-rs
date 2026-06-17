# XDP Backend Setup

`XdpFactoryBuilder` is the normal entry point for AF_XDP backends. Build the
factory once on a setup thread, then move each `XdpWorkerPlan` to the thread or
runtime task that will own the opened aggregate.

The factory controls three deployment choices:

- which NIC queues get AF_XDP sockets, through `claim`;
- how those queues are grouped into workers, through `threads`;
- which traffic the attached XDP program redirects, through `udp_ports`,
  `udp_port_range`, or `port_filter`.

Every example below seeds queue-local routes from netlink. Long-running workers
that need route updates should pair this with `XdpRouteMonitor`. Prepared UDP
endpoints cache the full L2+IPv4+UDP transmit header; route updates advance the
router generation, and the next endpoint send clears and rebuilds stale cached
headers before accepting packets.

## One Worker Per NIC Queue

Use this when each queue should have its own thread and aggregate. This is also
the default shape for evenly partitioned queues: if `threads` is omitted, the
factory creates one worker plan per claimed queue.

```rust,ignore
use std::net::SocketAddrV4;
use std::thread;

use fast_socket_xdp_rs::{
    InterfaceSelector, RouteSnapshot, XdpFactoryBuilder,
};

let local: SocketAddrV4 = "192.0.2.10:9000".parse()?;
let builder = XdpFactoryBuilder::new(InterfaceSelector::Name("eth0".to_string()))?;
let threads = builder.claimed_queue_count() as usize;

let factory = builder
    .threads(threads)
    .udp_ports([local.port()])
    .route_snapshot(RouteSnapshot::from_netlink()?)
    .build()?;

for plan in factory.into_worker_plans() {
    thread::spawn(move || -> Result<(), Box<dyn std::error::Error>> {
        let mut aggregate = plan.open_udp_busy_poll(local)?;
        run_worker(&mut aggregate)?;
        Ok(())
    });
}
```

Each worker plan pins its opening thread to `plan.cpu()` before opening the
aggregate. With one queue per worker, each aggregate contains one member socket.

## Several Queues On One Worker

Use this when the application wants fewer threads than queues. The worker opens
one aggregate with one member socket per assigned queue, and the application
drives those members from the same owner thread.

```rust,ignore
use std::net::SocketAddrV4;
use std::thread;

use fast_socket_xdp_rs::{
    InterfaceSelector, QueueClaim, RouteSnapshot, XdpFactoryBuilder,
};

let local: SocketAddrV4 = "192.0.2.10:9000".parse()?;

let factory = XdpFactoryBuilder::new(InterfaceSelector::Name("eth0".to_string()))?
    .claim(QueueClaim::First(4))
    .threads(1)
    .udp_ports([local.port()])
    .route_snapshot(RouteSnapshot::from_netlink()?)
    .build()?;

for plan in factory.into_worker_plans() {
    thread::spawn(move || -> Result<(), Box<dyn std::error::Error>> {
        let mut aggregate = plan.open_udp_busy_poll(local)?;
        while should_continue() {
            for socket in aggregate.members_mut() {
                app_step(socket)?;
            }
            aggregate.drain_tx_completions()?;
        }
        Ok(())
    });
}
```

`threads` must divide the claimed queue layout after the factory groups queues
by NUMA node. For a single-node NIC, `claim(QueueClaim::First(4)).threads(1)`
means one worker owns all four queues. `threads(2)` would produce two workers,
each with two queues.

## Flow-Steered Queue Subset

Use this when external flow steering sends the application's traffic to only
some NIC queues. Claim only that queue subset so the factory opens AF_XDP
sockets and XSKMAP entries for those queues.

```rust,ignore
use std::net::SocketAddrV4;
use std::thread;

use fast_socket_rs::QueueId;
use fast_socket_xdp_rs::{
    InterfaceSelector, QueueClaim, RouteSnapshot, XdpFactoryBuilder,
};

let local: SocketAddrV4 = "192.0.2.10:9000".parse()?;

let factory = XdpFactoryBuilder::new(InterfaceSelector::Name("eth0".to_string()))?
    .claim(QueueClaim::Queues(vec![QueueId::new(2), QueueId::new(3)]))
    .threads(2)
    .udp_ports([local.port()])
    .route_snapshot(RouteSnapshot::from_netlink()?)
    .build()?;

for plan in factory.into_worker_plans() {
    thread::spawn(move || -> Result<(), Box<dyn std::error::Error>> {
        let mut aggregate = plan.open_udp_busy_poll(local)?;
        run_worker(&mut aggregate)?;
        Ok(())
    });
}
```

On a non-bonded NIC, the queue ids passed to `QueueClaim::Queues` are the flat
NIC queue indices. The factory does not install the NIC steering rule; configure
that outside the library, then keep the factory claim in sync with the queues
that rule targets.

## One Socket Set For Several Ports

Use `udp_ports` when a worker should receive a small set of destination ports.
The factory attaches the bundled bound-ports XDP program and enables each port
before worker plans are opened.

```rust,ignore
use std::net::SocketAddrV4;

use fast_socket_xdp_rs::{
    InterfaceSelector, RouteSnapshot, XdpFactoryBuilder,
};

let local: SocketAddrV4 = "192.0.2.10:9000".parse()?;

let factory = XdpFactoryBuilder::new(InterfaceSelector::Name("eth0".to_string()))?
    .threads(2)
    .udp_ports([9000, 9001, 9002])
    .route_snapshot(RouteSnapshot::from_netlink()?)
    .build()?;
```

Use `udp_port_range(start, end)` instead when the redirected ports are
contiguous. That selects the bundled range-based XDP program and writes the
inclusive range at attach time:

```rust,ignore
use std::net::SocketAddrV4;

use fast_socket_xdp_rs::{
    InterfaceSelector, RouteSnapshot, XdpFactoryBuilder,
};

let local: SocketAddrV4 = "192.0.2.10:9000".parse()?;

let factory = XdpFactoryBuilder::new(InterfaceSelector::Name("eth0".to_string()))?
    .threads(2)
    .udp_port_range(9000, 9015)
    .route_snapshot(RouteSnapshot::from_netlink()?)
    .build()?;
```

`local` still provides the local IPv4 address and default source port used by
the opened UDP sockets. The receive-side accepted destination ports come from
the factory port filter. Open the worker plans with the same busy-poll or
wait-driven calls used in the previous scenarios.

## Runtime Owns Thread Placement

Use wait-driven aggregates and the unpinned openers when a runtime such as
Tokio decides where work runs. The factory still partitions queues and loads
the XDP program, but opening does not call `pin_current_thread_to_cpu`.

```rust,ignore
use std::net::SocketAddrV4;

use fast_socket_async_rs::{ActorConfig, spawn_udp_actor_local};
use fast_socket_xdp_rs::{
    InterfaceSelector, RouteSnapshot, XdpFactoryBuilder,
};

let local: SocketAddrV4 = "192.0.2.10:9000".parse()?;

let factory = XdpFactoryBuilder::new(InterfaceSelector::Name("eth0".to_string()))?
    .threads(2)
    .udp_ports([local.port()])
    .route_snapshot(RouteSnapshot::from_netlink()?)
    .build()?;

for plan in factory.into_worker_plans() {
    let aggregate = plan.open_udp_wait_driven_unpinned(local)?;
    for socket in aggregate.into_members() {
        spawn_udp_actor_local(
            socket,
            ActorConfig {
                recv_batch_size: 64,
                ..ActorConfig::default()
            },
        )?;
    }
}
```

Use the pinned `open_udp_wait_driven` variant when the worker is a dedicated
thread and should be pinned by the plan before opening the aggregate.
