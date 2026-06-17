# udp-proxy

The proxy program forwards UDP packets between clients and one upstream
endpoint. It remembers the most recent non-upstream sender as the client.
Packets from that client go to `--upstream`; packets from `--upstream` go back
to the remembered client.

There are two binaries:

- `udp-proxy` uses direct socket loops.
- `udp-tokio-proxy` uses Tokio socket actors.

This is a minimal forwarding example rather than a general NAT implementation:
it tracks only one latest client.

## Running

Direct OS socket:

```sh
cargo run -p fast-socket-examples --bin udp-proxy -- \
  --backend os \
  --device lo \
  --bind 192.168.0.10:9000 \
  --upstream 192.168.0.10:9001
```

Direct XDP busy-poll socket:

```sh
cargo run -p fast-socket-examples --bin udp-proxy -- \
  --backend xdp \
  --device eth0 \
  --bind 192.168.0.10:9000 \
  --upstream 192.168.0.20:9001 \
  --threads 1
```

Tokio OS actor:

```sh
cargo run -p fast-socket-examples --bin udp-tokio-proxy -- \
  --backend os \
  --device lo \
  --bind 192.168.0.10:9000 \
  --upstream 192.168.0.10:9001
```

Tokio XDP actor:

```sh
cargo run -p fast-socket-examples --bin udp-tokio-proxy -- \
  --backend xdp \
  --device eth0 \
  --bind 192.168.0.10:9000 \
  --upstream 192.168.0.20:9001 \
  --threads 1
```

## Direct Socket Implementation

`main` selects the OS loop or the XDP busy-poll loop and passes the configured
upstream address into each `ProxyState`. The runner repeatedly calls
`proxy_step` with the socket and its state:

```rust
while !shutdown_requested() {
    let count = proxy_step(&mut socket, &mut state)?;
    progress.add(count as u64);
    if count == 0 {
        socket.drain_tx_completions()?;
        thread::sleep(IDLE_SLEEP);
    }
}
```

`proxy_step` receives a batch, decides where each packet should go, and submits
the forwarded packets:

```rust
fn proxy_step<S>(socket: &mut S, state: &mut ProxyState<S>) -> Result<usize, BoxError>
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
        let destination = if item.meta.source == state.upstream {
            let Some(client) = state.last_client else {
                continue;
            };
            client
        } else {
            state.last_client = Some(item.meta.source);
            state.upstream
        };

        let mut tx = UdpTransmit::new(item.packet.freeze(), destination);
        tx.source_port = item.meta.destination_port;
        state.tx.push(TxSlot::Ready(tx));
    }

    let sent = common::send_all(socket, &mut state.tx)?;
    socket.drain_tx_completions()?;
    Ok(sent)
}
```

`ProxyState` owns one receive batch, one TX slot vector, the upstream address,
and `last_client`. Packets from the upstream are sent back to `last_client`;
packets from any other source update `last_client` and are sent to the
upstream. An upstream packet is skipped if no client has been observed yet.

The direct proxy does not allocate TX buffers. It freezes each received buffer
into a transmit buffer, changes the destination and optional source-port
metadata, and submits the ready slots through `common::send_all`.

## Tokio Actor Implementation

`udp-tokio-proxy` opens OS or wait-driven XDP actors and runs one `proxy_actor`
task per actor. The actor loop receives packets, chooses destinations, and
submits forwarded packets:

```rust
let mut last_client = None;
let mut tx_packets: Vec<ActorTxPacket<S>> = Vec::with_capacity(DEFAULT_BATCH_SIZE);

while !stop.load(Ordering::Relaxed) && !common::shutdown_requested() {
    let mut batch = match rx.recv_batch().await {
        Ok(batch) => batch,
        Err(_) => break,
    };

    tx_packets.clear();
    for packet in batch.drain() {
        let destination = if packet.meta.source == upstream {
            let Some(client) = last_client else {
                continue;
            };
            client
        } else {
            last_client = Some(packet.meta.source);
            upstream
        };
        let source_port = packet.meta.destination_port;
        let mut tx = packet.into_transmit(destination);
        tx.source_port = source_port;
        tx_packets.push(tx);
    }

    let sent = handle.send_tx_packets(&mut tx_packets).await?;
    total.fetch_add(sent as u64, Ordering::Relaxed);
}
```

The actor uses the same one-client forwarding rule as the direct implementation.
It does not allocate fresh TX buffers. Each forwarded packet is produced with
`packet.into_transmit(destination)`, which reuses the received payload buffer,
then submitted with `handle.send_tx_packets`.
