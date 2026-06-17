# udp-discard

The discard program receives UDP packets and drops them after counting progress.
It is the smallest receive-path example and is useful for checking bind,
batching, backend setup, and async actor plumbing without transmit work.

There are two binaries:

- `udp-discard` uses direct socket loops.
- `udp-tokio-discard` uses Tokio socket actors.

## Running

Direct OS socket:

```sh
cargo run -p fast-socket-examples --bin udp-discard -- \
  --backend os \
  --device lo \
  --bind 192.168.0.10:9000
```

Direct XDP busy-poll socket:

```sh
cargo run -p fast-socket-examples --bin udp-discard -- \
  --backend xdp \
  --device eth0 \
  --bind 192.168.0.10:9000 \
  --threads 1 \
  --batch-size 64
```

Tokio OS actor:

```sh
cargo run -p fast-socket-examples --bin udp-tokio-discard -- \
  --backend os \
  --device lo \
  --bind 192.168.0.10:9000
```

Tokio XDP actor:

```sh
cargo run -p fast-socket-examples --bin udp-tokio-discard -- \
  --backend xdp \
  --device eth0 \
  --bind 192.168.0.10:9000 \
  --threads 1
```

## Direct Socket Implementation

`main` selects `common::run_os_socket_loop` or
`common::run_xdp_busy_poll_loop`. The OS runner opens one socket and repeatedly
calls the example step function:

```rust,ignore
while !shutdown_requested() {
    let count = discard_step(&mut socket, &mut state)?;
    progress.add(count as u64);
    if count == 0 {
        socket.drain_tx_completions()?;
        thread::sleep(IDLE_SLEEP);
    }
}
```

The XDP runner uses the same `discard_step` callback, but calls it once per
socket in the busy-poll aggregate before deciding whether the worker made
progress:

```rust,ignore
while !worker_stop.load(Ordering::Relaxed) && !shutdown_requested() {
    let mut progressed = 0usize;
    for (socket, state) in aggregate.members_mut().iter_mut().zip(states.iter_mut()) {
        let count = discard_step(socket, state)?;
        progressed += count;
    }
    if progressed == 0 {
        aggregate.drain_tx_completions()?;
        thread::sleep(IDLE_SLEEP);
    }
}
```

The discard packet step is only a receive path:

```rust,ignore
fn discard_step<S>(socket: &mut S, state: &mut DiscardState<S>) -> Result<usize, BoxError>
where
    S: FastUdpSocket<RecvMeta = UdpRecvMeta>,
{
    state.rx.clear();
    let received = socket.recv(&mut state.rx)?;
    state.rx.clear();
    socket.drain_tx_completions()?;
    Ok(received)
}
```

`DiscardState` owns one reusable `RecvBatch`. Each pass clears stale handles,
receives into the batch, counts the packets, and clears the batch again.
Clearing the batch drops the packet handles so the socket can reuse those
buffers. No TX buffers are allocated, and no packets are sent.

## Tokio Actor Implementation

`udp-tokio-discard` opens one OS actor or a set of wait-driven XDP actors and
passes them to `common::run_actor_tasks`. The packet-processing loop is the
actor task:

```rust,ignore
while !stop.load(Ordering::Relaxed) && !common::shutdown_requested() {
    let batch = match rx.recv_batch().await {
        Ok(batch) => batch,
        Err(_) => break,
    };
    total.fetch_add(batch.len() as u64, Ordering::Relaxed);
}
```

The actor awaits a receive batch from `AsyncUdpRx`, adds its length to the
progress counter, and then lets the batch drop. Dropping the batch returns the
received buffers to the socket actor for reuse. The `AsyncUdpHandle` is unused
because discard does not allocate TX buffers or send packets.
