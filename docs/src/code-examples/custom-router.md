# Custom Router

The `custom-router` example demonstrates AF_XDP UDP construction with a
user-provided router. It sends one fixed 64-byte UDP payload per second to
`--target`, but it does not use the Linux route and neighbor tables for transmit
egress. Instead, every destination resolves through a route table whose next hop
is `0.0.0.0`, and an ARP table that returns the `--mac` address for every
next-hop lookup.

Run it with:

```bash
cargo run -p fast-socket-examples --bin custom-router -- \
  --device eth0 \
  --target 192.0.2.10:9000 \
  --mac 02:00:00:00:00:01
```

## Routing Tables

The route table is a small `RouteTable<V4Only>` implementation. It ignores the
destination IP and always returns the AF_XDP-bound interface with `0.0.0.0` as
the next hop.

```rust,ignore
#[derive(Clone, Copy, Debug)]
struct DefaultRouteTable {
    ifindex: IfIndex,
}

impl RouteTable<V4Only> for DefaultRouteTable {
    fn resolve_route(&self, _dst: Ipv4Addr) -> Option<RouteHop<Ipv4Addr>> {
        Some(RouteHop {
            ifindex: self.ifindex,
            next_hop: Ipv4Addr::UNSPECIFIED,
        })
    }
}
```

The ARP table is a matching `NeighborTable<V4Only>` implementation. It ignores
the next-hop IP and returns the MAC address supplied on the command line.

```rust,ignore
#[derive(Clone, Copy, Debug)]
struct ConstantArpTable {
    mac: LinkAddr,
}

impl NeighborTable<V4Only> for ConstantArpTable {
    fn resolve_l2(&self, _next_hop: Ipv4Addr) -> Option<LinkAddr> {
        Some(self.mac)
    }
}
```

The UDP router composes those two tables into an `XdpEgress`. The egress still
uses the socket's queue-local interface and queue from `XdpRouteContext`, plus
the source MAC and MTU read during setup.

```rust,ignore
#[derive(Clone, Copy, Debug)]
struct CustomRouter {
    routes: DefaultRouteTable,
    arp: ConstantArpTable,
    src_mac: LinkAddr,
    mtu: u32,
}

impl XdpUdpRouter for CustomRouter {
    fn resolve_udp_egress(&self, dst: Ipv4Addr, context: XdpRouteContext) -> Option<XdpEgress> {
        let route = self.routes.resolve_route(dst)?;
        if route.ifindex != context.ifindex {
            return None;
        }

        let dst_mac = self.arp.resolve_l2(route.next_hop)?;
        Some(XdpEgress::ipv4(
            context.ifindex,
            context.queue,
            dst_mac,
            self.src_mac,
            self.mtu.min(context.mtu as u32),
        ))
    }
}
```

## Socket Setup

`main` resolves queue 0 for `--device`, chooses a local IPv4 address and dynamic
source port, builds the custom router, and passes it to the UDP-level builder
with `.router(router)`.

```rust,ignore
let slot = resolve_xdp_queue_slot(&args.device, QueueId::new(0))?;
let local = SocketAddrV4::new(interface_ipv4_addr(&args.device)?, dynamic_source_port());
let mtu = interface_mtu(&slot.iface)?;
let router = CustomRouter {
    routes: DefaultRouteTable {
        ifindex: slot.ifindex,
    },
    arp: ConstantArpTable { mac: args.mac },
    src_mac: interface_mac(&slot.iface)?,
    mtu,
};

let mut socket = XdpUdpSocket::builder(slot.ifindex, slot.queue, local)
    .mtu(mtu as usize)
    .router(router)
    .open_busy_poll_live()?;
```

## Main Loop

The main loop sends one packet, reports the count, and sleeps for one second
until a shutdown signal arrives.

```rust,ignore
let payload = payload();
let mut sent = 0u64;

while !shutdown_requested() {
    send_one(&mut socket, target, &payload)?;
    sent = sent.saturating_add(1);
    eprintln!("custom-router: sent {sent} packets");
    thread::sleep(SEND_INTERVAL);
}

socket.drain_tx_completions()?;
```

`send_one` uses the generic `UdpSocket` transmit path. It allocates one TX
buffer, writes the fixed payload, freezes it, wraps it in `UdpTransmit`, and
submits a single-slot batch.

```rust,ignore
fn send_one<S>(socket: &mut S, target: SocketAddr, payload: &[u8; PAYLOAD_LEN]) -> Result<(), BoxError>
where
    S: UdpSocket,
{
    let mut tx_buffers = Vec::with_capacity(1);
    loop {
        tx_buffers.clear();
        if socket.allocate_tx_batch(&mut tx_buffers, 1)? == 0 {
            socket.drain_tx_completions()?;
            std::hint::spin_loop();
            continue;
        }

        let mut packet = tx_buffers.pop().expect("one TX buffer was allocated");
        packet.extend_from_slice(payload)?;
        let mut batch = [TxSlot::Ready(UdpTransmit::new(packet.freeze(), target))];
        match socket.send(&mut batch)? {
            1 => {
                socket.drain_tx_completions()?;
                return Ok(());
            }
            0 => {
                socket.drain_tx_completions()?;
                std::hint::spin_loop();
            }
            _ => unreachable!("single-packet batch cannot accept more than one packet"),
        }
    }
}
```
