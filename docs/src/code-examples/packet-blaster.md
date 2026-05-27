# Packet Blaster

The packet blaster sends fixed-size UDP payloads to one target as fast as the
backend accepts them. The full program chooses an OS-backed or AF_XDP-backed
socket in `main`, then passes it to generic `blaster`.

The hot loop uses `UdpSocket`, not a backend type:

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

Each payload is 64 bytes. `write_sequence` stores the current `u64` sequence in
the first eight bytes, giving receivers a cheap gap or reordering check. The
loop does not rate limit; it fills transmit buffers and submits batches until
shutdown.

The key API boundary is the transmit buffer pool. `allocate_tx_batch` fills
caller-owned scratch space with mutable buffers from the socket's `TxPool`. The
loop writes each UDP payload, freezes the buffer into the socket's immutable
transmit type, wraps it in `UdpTransmit`, and marks the slot `TxSlot::Ready`.

`send` accepts a prefix of submitted slots and consumes those packets by turning
their slots into `TxSlot::Taken`. A short accept is not an error, so the loop
rewinds the sequence number for the unaccepted tail and drains completions
before retrying. This preserves one sequence number per submitted packet while
honoring the batch API's prefix-accept contract.

`drain_tx_completions` stays in the generic loop even though the OS backend does
not need explicit transmit completions. For zero-copy backends such as AF_XDP,
it returns completed frames to the socket-owned pool so later
`allocate_tx_batch` calls can reuse them. The same loop can drive both sockets
without knowing the backend.
