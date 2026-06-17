# udp-echo

The echo program receives UDP payloads and sends each payload back to its
source. Both implementations demonstrate zero-copy forwarding: the receive
buffer becomes the transmit buffer instead of copying the payload into a fresh
allocation.

There are two binaries:

- `udp-echo` uses direct socket loops.
- `udp-tokio-echo` uses Tokio socket actors.

## Running

Direct OS socket:

```sh
cargo run -p fast-socket-examples --bin udp-echo -- \
  --backend os \
  --device lo \
  --bind 192.168.0.10:9000 \
  --batch-size 64 \
  --payload-capacity 2048
```

Direct XDP busy-poll socket:

```sh
cargo run -p fast-socket-examples --bin udp-echo -- \
  --backend xdp \
  --device eth0 \
  --bind 192.168.0.10:9000 \
  --threads 1
```

Tokio OS actor:

```sh
cargo run -p fast-socket-examples --bin udp-tokio-echo -- \
  --backend os \
  --device lo \
  --bind 192.168.0.10:9000 \
  --batch-size 64
```

Tokio XDP actor:

```sh
cargo run -p fast-socket-examples --bin udp-tokio-echo -- \
  --backend xdp \
  --device eth0 \
  --bind 192.168.0.10:9000 \
  --threads 1
```

## Direct Socket Implementation

`main` selects the OS loop or the XDP busy-poll loop. The OS runner repeatedly
calls `echo_step`; the XDP runner calls the same step once per socket in the
busy-poll aggregate.

```rust,ignore
while !shutdown_requested() {
    let count = echo_step(&mut socket, &mut state)?;
    progress.add(count as u64);
    if count == 0 {
        socket.drain_tx_completions()?;
        thread::sleep(IDLE_SLEEP);
    }
}
```

`echo_step` drains the receive batch into transmit slots:

```rust,ignore
fn echo_step<S>(socket: &mut S, state: &mut EchoState<S>) -> Result<usize, BoxError>
where
    S: FastUdpSocket<RecvMeta = UdpRecvMeta>,
    UdpRxBuffer<S>: PacketBufferMut<Frozen = UdpTxBuffer<S>>,
{
    state.rx.clear();
    if socket.recv(&mut state.rx)? == 0 {
        return Ok(0);
    }

    state.tx.clear();
    for item in state.rx.drain() {
        let mut tx = UdpTransmit::new(item.packet.freeze(), item.meta.source);
        tx.source_port = item.meta.destination_port;
        state.tx.push(TxSlot::Ready(tx));
    }

    let sent = socket.send_all(&mut state.tx)?;
    socket.notify_tx()?;
    socket.drain_tx_completions()?;
    Ok(sent)
}
```

`EchoState` owns a reusable receive batch and a reusable TX slot vector. Echo
does not call `allocate_tx_batch`. Each received packet is drained, frozen into
a transmit buffer, addressed back to `item.meta.source`, and submitted through
`socket.send_all`. The trait helper handles partial acceptance by draining TX
completions and retrying until every ready slot has been accepted. The example
then calls `notify_tx` for backends that need an explicit transmit wakeup.

## Tokio Actor Implementation

`udp-tokio-echo` opens OS or wait-driven XDP actors and runs one `echo_actor`
task per actor. The actor loop is the packet-processing loop:

```rust,ignore
let mut tx_packets: Vec<ActorTxPacket<S>> = Vec::with_capacity(DEFAULT_BATCH_SIZE);
while !stop.load(Ordering::Relaxed) && !common::shutdown_requested() {
    let mut batch = match rx.recv_batch().await {
        Ok(batch) => batch,
        Err(_) => break,
    };

    tx_packets.clear();
    for packet in batch.drain() {
        let source = packet.meta.source;
        let source_port = packet.meta.destination_port;
        let mut tx = packet.into_transmit(source);
        tx.source_port = source_port;
        tx_packets.push(tx);
    }

    let sent = handle.send_tx_packets(&mut tx_packets).await?;
    total.fetch_add(sent as u64, Ordering::Relaxed);
}
```

The actor awaits a receive batch, drains it, and converts each received packet
with `packet.into_transmit(source)`. That conversion reuses the received payload
buffer for the reply. The actor adjusts the optional source-port metadata and
submits the converted packets with `handle.send_tx_packets`.
