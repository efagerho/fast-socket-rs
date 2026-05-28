# Reflection Server

The reflection server demonstrates the `UdpSocket` API with one worker per NIC
queue. `pong-server` reflects each received UDP payload back to the sender.

Run it with either the OS or XDP backend:

```bash
cargo run -p fast-socket-examples --bin pong-server -- \
  --device eth0 \
  --target 192.0.2.20:9000 \
  --mode os
```

Use `--mode xdp` to run the AF_XDP-backed version.

## Generic Loop

The server loop is generic over `UdpSocket`. It receives UDP payloads, freezes
the received buffer, and sends the same bytes back to `UdpRecvMeta::source`.

```rust,ignore
fn pong_server<S>(
    socket: &mut S,
    stop: &AtomicBool,
    reflected: &AtomicU64,
) -> Result<(), BoxError>
where
    S: UdpSocket<RecvMeta = UdpRecvMeta>,
    UdpRxBuffer<S>: PacketBufferMut<Frozen = UdpTxBuffer<S>>,
{
    let mut rx: RecvBatch<UdpReceive<UdpRxBuffer<S>, UdpRecvMeta>> =
        RecvBatch::with_capacity(BATCH_SIZE);
    let mut tx: Vec<TxSlot<UdpTransmit<UdpTxBuffer<S>>>> =
        Vec::with_capacity(BATCH_SIZE);

    while !stop.load(Relaxed) && !shutdown_requested() {
        rx.clear();
        if socket.recv(&mut rx)? == 0 {
            socket.drain_tx_completions()?;
            continue;
        }

        tx.clear();
        for item in rx.drain() {
            tx.push(TxSlot::Ready(UdpTransmit::new(
                item.packet.freeze(),
                item.meta.source,
            )));
        }

        reflected.fetch_add(send_all(socket, &mut tx)? as u64, Relaxed);
        socket.drain_tx_completions()?;
    }

    Ok(())
}
```

`send_all` is the per-batch retry helper used by the loop. It calls
`socket.send` until every slot is accepted, draining completions and spinning
when the TX ring is momentarily full.

```rust,ignore
fn send_all<S>(
    socket: &mut S,
    batch: &mut [TxSlot<UdpTransmit<UdpTxBuffer<S>>>],
) -> Result<usize, BoxError>
where
    S: UdpSocket,
{
    let mut accepted = 0;
    while accepted < batch.len() {
        match socket.send(&mut batch[accepted..]) {
            Ok(0) => {
                socket.drain_tx_completions()?;
                std::hint::spin_loop();
            }
            Ok(n) => accepted += n,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(accepted)
}
```

The bound on `UdpRxBuffer<S>` says that a received mutable payload buffer can
freeze into the transmit buffer type for the same socket. That makes reflection
zero-copy for backends whose receive buffers can be transmitted directly.

## Queue Setup

The server discovers the device's RX queues and their interrupt CPUs. It then
creates one worker per queue and pins the worker to that CPU.

In OS mode, each worker creates a UDP socket bound to the same local address
with `SO_REUSEPORT`. It also sets `SO_INCOMING_CPU` to the queue CPU and binds
the socket to the requested device with `SO_BINDTODEVICE`. This demonstrates
how the OS backend can express queue affinity while still using `UdpSocket`.

In XDP mode, each worker opens one AF_XDP UDP socket for its queue and attaches
or reuses the interface-level XDP program. Each socket is still handed to the
same generic loop; only construction differs.

For `pong-server`, `--target` names the expected peer endpoint. The server binds
the device IP with `target.port()`. The XDP setup uses `target.ip()` to preflight
the queue-local route and seed the socket MTU; each reflected packet is still
sent back to the source address reported by `UdpRecvMeta`.
