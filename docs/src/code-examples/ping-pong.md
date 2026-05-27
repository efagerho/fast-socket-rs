# Ping/Pong

The ping/pong examples demonstrate the `UdpSocket` API with one worker per NIC
queue. `pong-server` reflects each received UDP payload back to the sender.
`ping-client` sends 64-byte pings and counts returned payloads.

Both programs take the same core arguments:

```bash
cargo run -p fast-socket-examples --bin pong-server -- \
  --device eth0 \
  --target 192.0.2.20:9000 \
  --mode os

cargo run -p fast-socket-examples --bin ping-client -- \
  --device eth0 \
  --target 192.0.2.10:9000 \
  --mode os
```

Use `--mode xdp` to run the AF_XDP-backed versions.

## Generic Server Loop

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

The bound on `UdpRxBuffer<S>` says that a received mutable payload buffer can
freeze into the transmit buffer type for the same socket. That makes reflection
zero-copy for backends whose receive buffers can be transmitted directly.

## Generic Client Loop

The client loop is also generic over `UdpSocket`. Each worker owns a sequence
space and writes the worker id plus sequence number into the payload. Returned
pongs may arrive on any queue, so each worker receives packets and forwards
acknowledgements to the worker that sent the original ping.

```rust,ignore
fn ping_client<S>(
    socket: &mut S,
    worker_id: usize,
    worker_count: usize,
    target: SocketAddrV4,
    ack_txs: &[mpsc::Sender<Ack>],
    ack_rx: &mpsc::Receiver<Ack>,
    stop: &AtomicBool,
    sent: &AtomicU64,
    received: &AtomicU64,
) -> Result<(), BoxError>
where
    S: UdpSocket<RecvMeta = UdpRecvMeta>,
{
    let mut sequence = 0u64;
    let mut outstanding = HashSet::with_capacity(MAX_OUTSTANDING);
    let mut payload = [0u8; PAYLOAD_LEN];
    let mut rx: RecvBatch<UdpReceive<UdpRxBuffer<S>, UdpRecvMeta>> =
        RecvBatch::with_capacity(BATCH_SIZE);

    while !stop.load(Relaxed) && !shutdown_requested() {
        drain_acks(ack_rx, &mut outstanding, received);

        if outstanding.len() < MAX_OUTSTANDING {
            write_ping_payload(&mut payload, worker_id, sequence);
            if send_one(socket, target.into(), &payload)? {
                outstanding.insert(sequence);
                sequence = sequence.wrapping_add(1);
                sent.fetch_add(1, Relaxed);
            }
        }

        rx.clear();
        if socket.recv(&mut rx)? > 0 {
            route_pong_acks::<S>(&mut rx, target, worker_count, ack_txs)?;
            socket.drain_tx_completions()?;
        } else {
            socket.drain_tx_completions()?;
        }
    }

    Ok(())
}
```

The acknowledgement channels are outside the socket API. They model a common
multi-queue rule: RSS or XDP redirect may deliver a response to a different
queue from the one that sent the request. The receiving worker reads the owner
id from the payload and sends an `Ack` to that owner. The owner removes the
sequence number from its outstanding set.

## Queue Setup

Both binaries discover the device's RX queues and their interrupt CPUs. They
then create one worker per queue and pin the worker to that CPU.

In OS mode, each worker creates a UDP socket bound to the same local address
with `SO_REUSEPORT`. It also sets `SO_INCOMING_CPU` to the queue CPU and binds
the socket to the requested device with `SO_BINDTODEVICE`. This demonstrates
how the OS backend can express queue affinity while still using `UdpSocket`.

In XDP mode, each worker opens one AF_XDP UDP socket for its queue and attaches
or reuses the interface-level XDP program. Each socket is still handed to the
same generic loop; only construction differs.

For `pong-server`, `--target` names the expected peer endpoint. The server binds
the device IP with `target.port()`. The current direct XDP UDP constructor also
needs an egress peer during construction, so the example uses `target.ip()` for
that egress setup.
