# Packet Blaster

The packet blaster sends fixed-size UDP payloads to one target as quickly as the
selected backend can accept them. The complete example program chooses either an
OS-backed or AF_XDP-backed socket in `main`, then hands that concrete socket to a
generic `blaster` function.

The hot loop can be written against the `UdpSocket` trait instead of either
backend type:

```rust,ignore
use std::net::SocketAddr;

use fast_socket_rs::{
    PacketBufferMut, TxSlot, UdpSocket, UdpTransmit, UdpTxBuffer, UdpTxBufferMut,
};

const PAYLOAD_LEN: usize = 64;
const BATCH_SIZE: usize = 64;

fn blaster<S>(socket: &mut S, target: SocketAddr) -> Result<(), BoxError>
where
    S: UdpSocket,
{
    let mut sequence = 0u64;
    let mut payload = [0u8; PAYLOAD_LEN];
    let mut tx_buffers: Vec<UdpTxBufferMut<S>> = Vec::with_capacity(BATCH_SIZE);
    let mut batch: Vec<TxSlot<UdpTransmit<UdpTxBuffer<S>>>> =
        Vec::with_capacity(BATCH_SIZE);

    while !shutdown_requested() {
        tx_buffers.clear();
        batch.clear();

        socket.allocate_tx_batch(&mut tx_buffers, BATCH_SIZE)?;

        while let Some(mut packet) = tx_buffers.pop() {
            write_sequence(&mut payload, sequence);
            packet.extend_from_slice(&payload)?;
            batch.push(TxSlot::Ready(UdpTransmit::new(packet.freeze(), target)));
            sequence = sequence.wrapping_add(1);
        }

        if batch.is_empty() {
            socket.drain_tx_completions()?;
            std::hint::spin_loop();
            continue;
        }

        let accepted = socket.send(batch.as_mut_slice())?;
        if accepted < batch.len() {
            let rejected = batch.len() - accepted;
            sequence = sequence.wrapping_sub(rejected as u64);
            socket.drain_tx_completions()?;
        }
    }

    Ok(())
}
```

Each payload is exactly 64 bytes. `write_sequence` stores the current `u64`
sequence number in the first eight bytes, which gives receivers a cheap way to
spot gaps or reordering without changing the socket API. The loop does not rate
limit; it keeps filling transmit buffers and submitting batches until shutdown
is requested.

The important API boundary is the transmit buffer pool. `allocate_tx_batch`
fills caller-owned scratch space with mutable buffers from the socket's
`TxPool`. The loop writes the UDP payload into each buffer, freezes it into the
socket's immutable transmit buffer type, wraps it in `UdpTransmit`, and marks the
slot as `TxSlot::Ready`.

`send` accepts a prefix of the submitted slots and consumes those packets by
turning their slots into `TxSlot::Taken`. A short accept is not an error by
itself, so the loop rewinds the sequence number for the unaccepted tail and
drains transmit completions before trying again. That preserves the simple
one-sequence-per-successfully-submitted-packet model while still honoring the
batch API's prefix-accept contract.

`drain_tx_completions` is part of the generic loop even though the OS backend
does not need explicit transmit completion work. For zero-copy backends such as
AF_XDP, it returns completed frames to the socket-owned pool so later
`allocate_tx_batch` calls can reuse them. This is what lets the same loop run
over both concrete socket implementations without knowing which backend it is
driving.
