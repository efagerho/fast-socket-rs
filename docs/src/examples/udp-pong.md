# udp-pong

The pong program responds to every received UDP packet with a generated payload
of a fixed length. Unlike echo and proxy, pong allocates fresh transmit buffers
and writes response bytes into them.

There are two binaries:

- `udp-pong` uses direct socket loops.
- `udp-tokio-pong` uses Tokio socket actors.

## Running

Direct OS socket:

```sh
cargo run -p fast-socket-examples --bin udp-pong -- \
  --backend os \
  --device lo \
  --bind 192.168.0.10:9000 \
  --response-len 64
```

Direct XDP busy-poll socket:

```sh
cargo run -p fast-socket-examples --bin udp-pong -- \
  --backend xdp \
  --device eth0 \
  --bind 192.168.0.10:9000 \
  --threads 1 \
  --batch-size 64 \
  --response-len 64
```

Tokio OS actor:

```sh
cargo run -p fast-socket-examples --bin udp-tokio-pong -- \
  --backend os \
  --device lo \
  --bind 192.168.0.10:9000 \
  --response-len 64
```

Tokio XDP actor:

```sh
cargo run -p fast-socket-examples --bin udp-tokio-pong -- \
  --backend xdp \
  --device eth0 \
  --bind 192.168.0.10:9000 \
  --threads 1 \
  --response-len 64
```

## Direct Socket Implementation

`main` builds the fixed response payload, then selects the OS loop or the XDP
busy-poll loop. The runner repeatedly calls `pong_step` for each active socket:

```rust
while !shutdown_requested() {
    let count = pong_step(&mut socket, &mut state)?;
    progress.add(count as u64);
    if count == 0 {
        socket.drain_tx_completions()?;
        thread::sleep(IDLE_SLEEP);
    }
}
```

`pong_step` receives a batch, allocates TX buffers, fills them, and sends the
generated replies:

```rust
fn pong_step<S>(socket: &mut S, state: &mut PongState<S>) -> Result<usize, BoxError>
where
    S: FastUdpSocket<RecvMeta = UdpRecvMeta>,
{
    state.rx.clear();
    let received = socket.recv(&mut state.rx)?;
    if received == 0 {
        return Ok(0);
    }

    state.tx_buffers.clear();
    state.tx.clear();
    socket.allocate_tx_batch(&mut state.tx_buffers, received)?;

    for item in state.rx.drain() {
        let Some(mut buffer) = state.tx_buffers.pop() else {
            break;
        };
        buffer.extend_from_slice(&state.response)?;
        let mut tx = UdpTransmit::new(buffer.freeze(), item.meta.source);
        tx.source_port = item.meta.destination_port;
        state.tx.push(TxSlot::Ready(tx));
    }

    let sent = common::send_all(socket, &mut state.tx)?;
    socket.drain_tx_completions()?;
    Ok(sent)
}
```

`PongState` owns a receive batch, a scratch vector for mutable TX buffers, a TX
slot vector, and the shared response bytes. `socket.recv` determines how many
responses are needed. `socket.allocate_tx_batch` asks for up to that many TX
buffers. If fewer buffers are available than packets were received, the step
sends only the responses it could allocate. `common::send_all` submits the
ready slots and retries partial sends.

## Tokio Actor Implementation

`udp-tokio-pong` opens OS or wait-driven XDP actors and runs one `pong_actor`
task per actor. The actor loop records reply destinations, allocates actor TX
buffers, fills them, and submits generated packets:

```rust
let mut destinations: Vec<(SocketAddr, Option<u16>)> = Vec::with_capacity(batch_size);
let mut tx_buffers: Vec<ActorTxBuffer<S>> = Vec::with_capacity(batch_size);
let mut tx_packets: Vec<ActorTxPacket<S>> = Vec::with_capacity(batch_size);

while !stop.load(Ordering::Relaxed) && !common::shutdown_requested() {
    let mut batch = match rx.recv_batch().await {
        Ok(batch) => batch,
        Err(_) => break,
    };

    destinations.clear();
    for packet in batch.drain() {
        destinations.push((packet.meta.source, packet.meta.destination_port));
    }

    tx_packets.clear();
    while tx_packets.len() < destinations.len()
        && !stop.load(Ordering::Relaxed)
        && !common::shutdown_requested()
    {
        tx_buffers.clear();
        let allocated = handle
            .alloc_tx_batch(destinations.len() - tx_packets.len(), &mut tx_buffers)
            .await?;
        if allocated == 0 {
            tokio::task::yield_now().await;
            continue;
        }

        for mut buffer in tx_buffers.drain(..) {
            let Some((destination, source_port)) = destinations.get(tx_packets.len()).copied()
            else {
                break;
            };
            buffer.buffer_mut().extend_from_slice(&response)?;
            let mut packet = buffer.freeze(destination);
            packet.source_port = source_port;
            tx_packets.push(packet);
        }
    }

    let sent = handle.send_tx_packets(&mut tx_packets).await?;
    total.fetch_add(sent as u64, Ordering::Relaxed);
}
```

The actor keeps only the sender address and destination-port metadata from each
received packet. It then repeatedly calls `handle.alloc_tx_batch` until it has a
reply for every destination, yielding if no buffers are temporarily available.
Each allocated buffer is filled with the generated response, frozen for the
destination, and submitted with `handle.send_tx_packets`.
