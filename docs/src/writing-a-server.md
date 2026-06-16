# Writing a Server

An application writer has two main ways to build a UDP server with this
workspace:

- drive a socket directly through the core `UdpSocket` API;
- use UDP tiles and let a tile worker own the sockets.

Both choices use the same packet ownership model. Sockets own packet pools,
received packets arrive in owned buffers, transmit packets are frozen buffers in
`TxSlot`s, and sockets must outlive buffers handed out by their pools.

The difference is where the worker loop lives.

## Choosing an API

Use the direct socket API when the server needs exact control over socket
threads. The application decides which thread owns each socket, when the thread
pins itself, how it waits or busy-polls, when route maintenance runs, how
transmit back-pressure is handled, and when completions are drained.

Use tiles when the server wants a network worker to own those details. The tile
runtime drives one or more sockets, classifies receive packets into application
lanes, keeps per-lane transmit-buffer queues filled, accepts transmit work from
lanes, and handles the socket polling mode.

Direct sockets are lower level and more flexible. Tiles are higher level and
usually easier to compose with application worker threads.

## Direct Socket Servers

With the direct API, a server owns a concrete socket such as
`fast_socket_os_rs::OsUdpSocket` or an AF_XDP UDP socket from
`fast_socket_xdp_rs`. The server loop calls the socket methods itself:

- `recv` to fill a reusable `RecvBatch`;
- `send` or `send_all` to submit transmit slots;
- `drain_tx_completions` to reclaim transmit buffers;
- `notify_tx` when a backend requires an explicit transmit kick;
- `driver_mut().wait(...)` for wait-driven sockets, or a busy-poll policy for
  busy-poll sockets.

This is the right shape for a server that wants a tight per-queue loop, a custom
reactor, a benchmark-style data path, or backend-specific maintenance in a known
place in the loop.

The cost is that the application owns more machinery. It must preserve pending
transmit slots after partial sends, decide when to drop or retry queued work,
drain completions often enough to keep pools healthy, run route updates for XDP
workers, and make sure socket lifetimes dominate buffer lifetimes.

This skeleton shows the direct UDP echo shape:

```rust,ignore
use std::time::Duration;

use fast_socket_rs::{
    Error, PacketBufferMut, PollDriver, PollMode, RecvBatch, TxSlot, UdpReceive,
    UdpRecvMeta, UdpRxBuffer, UdpSocket, UdpTransmit, UdpTxBuffer,
};

fn echo_loop<S>(socket: &mut S) -> Result<(), Error>
where
    S: UdpSocket<RecvMeta = UdpRecvMeta>,
    UdpRxBuffer<S>: PacketBufferMut<Frozen = UdpTxBuffer<S>>,
{
    let mut rx: RecvBatch<UdpReceive<UdpRxBuffer<S>, UdpRecvMeta>> =
        RecvBatch::with_capacity(64);
    let mut pending: Vec<TxSlot<UdpTransmit<UdpTxBuffer<S>>>> =
        Vec::with_capacity(64);

    loop {
        let mut progressed = false;

        rx.clear();
        if socket.recv(&mut rx)? != 0 {
            progressed = true;
            for packet in rx.drain() {
                let mut tx =
                    UdpTransmit::new(packet.packet.freeze(), packet.meta.source);
                tx.source_port = packet.meta.destination_port;
                pending.push(tx.into());
            }
        }

        progressed |= flush_pending(socket, &mut pending)? != 0;
        progressed |= socket.drain_tx_completions()? != 0;

        if !progressed {
            match <S::Driver as PollDriver>::MODE {
                PollMode::WaitDriven => {
                    let _ = socket
                        .driver_mut()
                        .wait(Some(Duration::from_millis(1)))?;
                }
                PollMode::BusyPoll => std::hint::spin_loop(),
                _ => {}
            }
        }
    }
}

fn flush_pending<S>(
    socket: &mut S,
    pending: &mut Vec<TxSlot<UdpTransmit<UdpTxBuffer<S>>>>,
) -> Result<usize, Error>
where
    S: UdpSocket,
{
    let mut total = 0;
    while !pending.is_empty() {
        match socket.send(pending.as_mut_slice()) {
            Ok(0) => break,
            Ok(accepted) => {
                pending.drain(..accepted);
                total += accepted;
            }
            Err(error) => {
                if error.accepted != 0 {
                    pending.drain(..error.accepted);
                    total += error.accepted;
                }
                return Err(error.kind);
            }
        }
    }
    Ok(total)
}
```

For generated responses rather than echoes, allocate mutable buffers from the
socket's transmit pool:

```rust,ignore
use std::net::SocketAddr;

use fast_socket_rs::{
    Error, PacketBufferMut, TxSlot, UdpSocket, UdpTransmit, UdpTxBuffer,
    UdpTxBufferMut,
};

fn queue_payload<S>(
    socket: &mut S,
    scratch: &mut Vec<UdpTxBufferMut<S>>,
    pending: &mut Vec<TxSlot<UdpTransmit<UdpTxBuffer<S>>>>,
    destination: SocketAddr,
    payload: &[u8],
) -> Result<bool, Error>
where
    S: UdpSocket,
{
    scratch.clear();
    if socket.allocate_tx_batch(scratch, 1)? == 0 {
        return Ok(false);
    }

    let mut buffer = scratch
        .pop()
        .expect("allocate_tx_batch reported one allocated buffer");
    buffer
        .extend_from_slice(payload)
        .map_err(|_| Error::InvalidPacket)?;

    pending.push(UdpTransmit::new(buffer.freeze(), destination).into());
    Ok(true)
}
```

## Tile Servers

A UDP tile moves the socket loop into a dedicated worker. Application lanes use
handles instead of calling `UdpSocket::recv` or `UdpSocket::send` directly.

The tile worker:

- owns the socket set;
- drains socket completions;
- receives from sockets into batches;
- classifies received packets to lane RX queues;
- refills per-lane transmit-buffer queues;
- drains lane TX queues and submits packets to sockets;
- parks or spins according to the socket polling mode.

The application lane:

- pops `TileRxBatch` values;
- processes or forwards `TileRxPacket` values;
- allocates `TileTxBuffer` values from its lane handle when it needs fresh
  transmit storage;
- pushes filled buffers with shared `TileTxMeta`, or pushes frozen
  `TileTxPacket` values when each packet has distinct metadata.

This shape is useful when socket ownership should stay pinned to NIC queues or
UMEM owner threads, while application work is split across one or more lanes.
The bounded queues make back-pressure explicit: push methods return how much
work was accepted and leave unaccepted work in the caller's vector.

This skeleton reflects received packets from one lane handle:

```rust,ignore
use fast_socket_rs::{PacketBufferMut, UdpRecvMeta, UdpRxBuffer, UdpSocket, UdpTxBuffer};
use fast_socket_udp_tile::{TileTxPacket, UdpNetworkTileHandle};

fn reflect_lane<H>(
    handle: &mut H,
    tx_packets: &mut Vec<TileTxPacket<H::Socket>>,
) -> (usize, usize)
where
    H: UdpNetworkTileHandle,
    H::Socket: UdpSocket<RecvMeta = UdpRecvMeta>,
    UdpRxBuffer<H::Socket>: PacketBufferMut<Frozen = UdpTxBuffer<H::Socket>>,
{
    let mut queued = 0;
    let mut dropped = 0;

    while let Some(mut batch) = handle.pop_rx_batch() {
        tx_packets.clear();
        for packet in batch.drain() {
            let mut tx = packet.into_transmit(packet.meta().source);
            tx.source_port = packet.meta().destination_port;
            tx_packets.push(tx);
        }
        handle.recycle_rx_batch(batch);

        queued += handle.push_tx_packets(tx_packets);
        if !tx_packets.is_empty() {
            dropped += tx_packets.len();
            tx_packets.clear();
        }
    }

    (queued, dropped)
}
```

The backend tile crates provide the common construction paths. AF_XDP tiles can
discover queues, seed routes, install UDP port filters, start route monitoring,
and create one tile worker per worker plan:

```rust,ignore
use fast_socket_udp_tile::SourceAddrClassifier;
use fast_socket_udp_tile_xdp::XdpUdpTileBuilder;

let mut tiles = XdpUdpTileBuilder::bind_device(device, local, lane_count)?
    .threads(threads)
    .classifier(SourceAddrClassifier)
    .build()?;

let lane_handles = tiles.lane_handles(0).expect("lane exists");
```

OS tiles wrap repeated `SO_REUSEPORT` sockets and use parked wait-driven tile
workers:

```rust,ignore
use std::sync::Arc;

use fast_socket_udp_tile_os::{OsUdpTileBuilder, UdpNetworkTile};

let tile = OsUdpTileBuilder::reuse_port(bind_addr, socket_count, lane_count)
    .build();

let lane0 = Arc::clone(&tile).lane_handle(0).expect("lane exists");
let worker = Arc::clone(&tile).start(0)?;
```

## Tradeoffs

Choose direct sockets when the socket loop is part of the application design.
That usually means one worker owns one queue or aggregate, the packet path is
tightly tuned, the server needs IP packet sockets as well as UDP sockets, or the
application wants exact control over waiting, spinning, completion draining, and
maintenance.

Choose tiles when the application wants to separate network ownership from
application lanes. Tiles reduce repeated socket-loop code, centralize
back-pressure points, keep transmit buffers preallocated for lanes, and hide
backend worker details behind lane handles.

The tile cost is an extra queueing layer and less direct control over exactly
when socket operations occur. The direct-socket cost is that the application has
to write and maintain the loop correctly.

In both designs, reuse batches and vectors, handle partial transmit acceptance,
drain completions regularly, and keep sockets alive until all buffers from their
pools have been returned or dropped.
