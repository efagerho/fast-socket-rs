# UDP Tile API

The UDP tile API is for programs that want one worker thread to own one or more
UDP sockets while application lanes exchange packets with that worker through
bounded queues.

This is useful when a backend socket should stay on a specific CPU, NIC queue,
or UMEM owner thread, but packet processing is split across other producer or
lane threads. Lanes never call `UdpSocket::send` or `UdpSocket::recv` directly.
They receive tile-delivered batches, allocate tile-owned transmit buffers, and
push filled transmit work through lane handles.

The API is split across crates:

- `fast-socket-udp-tile` contains backend-independent contracts and packet
  wrappers.
- `fast-socket-udp-tile-xdp` contains the AF_XDP tile runtime.
- `fast-socket-udp-tile-os` contains the OS-socket tile runtime.

## Thread Model

A tile worker owns its socket set. The worker loop performs socket maintenance,
drains lane TX work queues, submits pending transmits, drains completions,
refills the lane-facing transmit-buffer pools, and receives packets from
sockets.

Application lanes interact with the worker through handles returned by the tile.
Each handle owns one lane index internally. Handles are `Send`, so they can move
into lane threads, and backend handle types are intentionally not `Sync` so the
type reflects the single-consumer lane model.

The same buffer lifetime rule applies as in the core socket API: sockets must
outlive buffers handed out by their pools. A tile satisfies that rule by owning
the sockets for the lifetime of the worker thread and by only handing lanes
buffers that came from those sockets.

## Core Types

`SocketIndex` is a stable index into the tile-owned socket set. Packets carry
the index of the socket that produced or allocated their backing buffer.

`TileRxPacket<S>` is delivered from the tile to a lane RX queue. It contains
backend receive metadata, the received UDP payload buffer, and the source socket
index. When the receive buffer can freeze into the socket's TX buffer type,
`TileRxPacket::into_transmit` converts the received packet into a
`TileTxPacket<S>` for forwarding or reflection.

`TileRxBatch<S>` is the lane receive unit. It owns a vector of
`TileRxPacket<S>` and keeps that allocation reusable. A lane that wants to
handle one packet at a time still pops a batch and iterates over the packets in
that batch.

`TileTxBuffer<S>` is a mutable transmit buffer allocated by a tile-owned socket.
Lanes fill these buffers and submit them with shared `TileTxMeta`.

`TileTxMeta` is transmit metadata applied to a group of `TileTxBuffer<S>`
values. It carries the destination address, optional source IP, optional source
port, ECN, and GSO segment size.

`TileTxPacket<S>` is one frozen UDP payload plus per-packet transmit metadata.
It is the right type for reflection and forwarding, where each packet may have a
different destination or source port.

## Classifiers

An `IngressClassifier<M, B>` maps received packets to lane RX queues:

```rust,ignore
fn classify(&self, meta: &M, packet: &B, rx_queue_count: usize) -> IngressDecision;
```

The classifier returns either `IngressDecision::Deliver(index)` or
`IngressDecision::Drop`.

The shared crate includes two simple classifiers:

- `AcceptAllClassifier`, which sends every packet to lane 0;
- `SourceAddrClassifier`, which hashes the UDP source address across lanes.

Backend tiles own the classifier and call it on the worker thread after
receiving from sockets.

## Lane Handle Interface

`UdpNetworkTileHandle` is the shared per-lane interface used by application
threads:

```rust,ignore
pub trait UdpNetworkTileHandle: Send + 'static {
    type Socket: UdpSocket;

    fn lane_index(&self) -> usize;
    fn pop_rx_batch(&self) -> Option<TileRxBatch<Self::Socket>>;

    fn push_tx_buffers(
        &mut self,
        buffers: &mut Vec<TileTxBuffer<Self::Socket>>,
        meta: TileTxMeta,
    ) -> usize;

    fn push_tx_packets(
        &mut self,
        packets: &mut Vec<TileTxPacket<Self::Socket>>,
    ) -> usize;

    fn alloc_tx_buffers(
        &mut self,
        count: usize,
        out: &mut Vec<TileTxBuffer<Self::Socket>>,
    ) -> usize;

    fn alloc_rx_batch(&self) -> TileRxBatch<Self::Socket>;
    fn recycle_rx_batch(&self, batch: TileRxBatch<Self::Socket>);
}
```

`pop_rx_batch`, `push_tx_buffers`, and `push_tx_packets` operate on the
handle's lane. The caller does not pass a lane index on the hot path.

`push_tx_buffers` accepts a mutable vector of filled tile TX buffers plus shared
metadata. `push_tx_packets` accepts a mutable vector of frozen packet objects.
Both methods return the number of items accepted by the tile. Accepted items are
removed from the vector; any remaining items stay in the vector and are still
owned by the caller. Dropping those remaining items drops the packets and
eventually returns their buffers through the backend's normal reclaim path.

`alloc_tx_buffers` pops preallocated buffers from the SPSC pool for the handle's
lane. The tile worker refills one buffer pool per lane, so producers do not
contend on a shared mutable-buffer queue. The method may return fewer than
requested, including zero.

`alloc_rx_batch` and `recycle_rx_batch` manage reusable receive batch
containers. Lanes should return receive batches after draining them.

## Tile Interface

`UdpNetworkTile` is the shared interface used to configure, inspect, and start a
tile:

```rust,ignore
pub trait UdpNetworkTile: Send + Sync + 'static {
    type Socket: UdpSocket;
    type Handle: UdpNetworkTileHandle<Socket = Self::Socket>;

    fn lane_handle(self: Arc<Self>, lane_index: usize) -> Option<Self::Handle>;

    fn stats(&self) -> TileStats;

    fn start(
        self: Arc<Self>,
        tile_index: usize,
    ) -> Result<JoinHandle<Result<(), TileError>>, TileError>;
}
```

`lane_handle` creates the per-lane API object for one lane. It returns `None`
when the lane index is outside the configured lane count. Application code uses
the returned handle for lane-local RX, TX, and reusable buffer management.

`stats` reports tile-side drops and transmits accepted by tile-owned sockets.
Full lane TX work queues are visible to the lane because the push methods leave
unaccepted work in the caller's vector.

`start` consumes an `Arc<Self>` and starts the worker thread. It can only be
called once per tile.

## Producer Shape

A high-rate producer should reuse a local `Vec<TileTxBuffer<_>>`, allocate a
group of transmit buffers, fill them, and push the vector once:

```rust,ignore
use std::net::SocketAddr;

use fast_socket_rs::PacketBufferMut;
use fast_socket_udp_tile::{TileTxBuffer, TileTxMeta, UdpNetworkTileHandle};

fn queue_payloads<H>(
    handle: &mut H,
    destination: SocketAddr,
    payloads: &[&[u8]],
    buffers: &mut Vec<TileTxBuffer<H::Socket>>,
) -> Result<usize, String>
where
    H: UdpNetworkTileHandle,
{
    buffers.clear();
    let allocated = handle.alloc_tx_buffers(payloads.len(), buffers);
    if allocated == 0 {
        return Ok(0);
    }

    for (buffer, payload) in buffers.iter_mut().zip(payloads.iter()) {
        buffer
            .extend_from_slice(payload)
            .map_err(|error| error.to_string())?;
    }

    let accepted = handle.push_tx_buffers(buffers, TileTxMeta::new(destination));
    buffers.clear();
    Ok(accepted)
}
```

This shape amortizes the lane-to-tile queue operation across the whole vector.
If `push_tx_buffers` accepts only part of the vector, the remaining buffers are
still in `buffers`; the example drops them with `clear`.

## Consumer Shape

A lane drains receive work by popping batches. It can process the packets in the
batch however it likes, then return the empty container through the handle:

```rust,ignore
use fast_socket_udp_tile::UdpNetworkTileHandle;

fn drain_rx<H>(handle: &H)
where
    H: UdpNetworkTileHandle,
{
    while let Some(mut batch) = handle.pop_rx_batch() {
        for packet in batch.drain() {
            handle_packet(packet);
        }
        handle.recycle_rx_batch(batch);
    }
}
# fn handle_packet<T>(_packet: T) {}
```

Reflection servers usually reuse a local `Vec<TileTxPacket<_>>` and submit
frozen packets:

```rust,ignore
while let Some(mut batch) = handle.pop_rx_batch() {
    packets.clear();
    for packet in batch.drain() {
        let destination = packet.meta().source;
        let source_port = packet.meta().destination_port;
        let mut tx = packet.into_transmit(destination);
        tx.source_port = source_port;
        packets.push(tx);
    }
    handle.recycle_rx_batch(batch);

    let accepted = handle.push_tx_packets(&mut packets);
    packets.clear();
    reflected += accepted;
}
```

## Backend Builders

The backend crates provide builders for the common tile shapes so applications
do not have to name the socket-set wrapper or tile polling mode.

`fast-socket-udp-tile-xdp::XdpUdpTileBuilder` has a high-level device path for
application code. `bind_device` discovers the interface queues, seeds the route
snapshot from netlink, installs a UDP destination-port filter for the local bind
port by default, wires route-monitor maintenance into the tile workers, and
starts one busy-poll tile per worker plan. The returned `XdpUdpTiles<C>` set can
create lane handles, report summed tile stats, and check whether any tile worker
exited unexpectedly.

```rust,ignore
let mut tiles = XdpUdpTileBuilder::bind_device(device, local, lane_count)?
    .threads(threads)
    .classifier(SourceAddrClassifier)
    .build()?;

let lane_handles = tiles.lane_handles(0).expect("lane exists");
```

Use `udp_ports` or `udp_port_range` before `build` when one tile should receive
multiple local UDP ports. The `local` port remains the default transmit source
port; set `TileTxMeta::source_port` or `TileTxPacket::source_port` when packets
should leave from a different bound port. Received `UdpRecvMeta` includes
`destination_port` when the backend can report it, which is useful for
reflection servers.

`XdpUdpTileBuilder::new` remains available for advanced integrations that
already built an `XdpFactory` and want to pass it directly to the tile layer.

`fast-socket-udp-tile-os::OsUdpTileBuilder` wraps the common OS shape. The
low-level constructor accepts a factory that returns `Vec<OsUdpSocket>`. For
the common repeated-socket case, `reuse_port` creates a builder that opens
multiple `SO_REUSEPORT` sockets through `OsUdpSocketBuilder`; use `bind_device`
for Linux `SO_BINDTODEVICE` and `configure_socket` to set per-socket affinity,
MTU, or buffer layout. `build` creates a wait-driven parked tile.

The shared tile crate exposes:

- `Spin`, a busy-poll tile mode for busy-poll sockets;
- `Park`, a parked tile mode for wait-driven sockets;
- `UdpSocketSet`, the trait implemented by socket collections the tile can
  drive.

The queue implementation and SPSC endpoints are backend internals. Application
code interacts with them only through `UdpNetworkTileHandle`.

The concrete generic runtime remains available as `UdpTile<Set, M, C>` for
advanced integrations. `Set` is a backend-specific socket set, `M` is a
`TilePollMode`, and `C` is the ingress classifier. `Spin` is only implemented
for busy-poll socket drivers; `Park` is only implemented for wait-driven socket
drivers that expose wake handles.

The XDP backend provides `XdpUdpSocketSet` for advanced callers that already
own an `XdpUdpAggregate`. Aggregates opened by the XDP factory use one shared
UMEM per worker plan with separate queue-local rings. In that case, the XDP
tile can submit a buffer through a different member socket in the same
aggregate and reclaim it on the completing ring. That lets the tile spread
transmit work across member TX rings.

The OS backend keeps the conservative source-socket behavior. Packets are sent
through the socket that allocated or received their backing buffer.

## Configuration

`TileConfig` controls queue and batch sizing:

- `queue_capacity` is the target packet capacity for each lane TX work queue;
- `batch_size` is the receive and transmit batch size used by the worker;
- `tx_buffer_queue_capacity` is the number of preallocated transmit buffers
  exposed to each lane;
- `tx_buffer_refill_watermark` starts worker-side TX-buffer refills for a lane;
- `tx_buffer_refill_batch` limits one refill pass for a lane;
- `pin_thread` controls whether the worker pins itself using socket affinity
  hints.

## Performance Notes

Reuse caller-owned vectors on hot paths. The push methods accept mutable
vectors so lanes can keep allocation ownership and submit many packets with one
queue operation.

Use `push_tx_buffers` when all packets share destination/source metadata. Use
`push_tx_packets` when forwarding or reflection needs per-packet metadata.

Keep `batch_size` aligned with the backend's socket send batch size. For AF_XDP,
using full vectors lets the socket submit many descriptors per ring commit and
reduces transmit doorbells.

Watch the cross-thread queues separately when tuning:

- one SPSC TX-buffer pool per lane feeds mutable buffers from the tile worker to
  that lane's handle;
- one SPSC TX-work queue per lane feeds filled buffers or frozen packets back to
  the worker.

If a per-lane TX-buffer pool is hot in profiles, the next optimization is
usually to move buffers between the worker and lanes in larger groups.
