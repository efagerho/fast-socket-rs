# Buffer Ownership

Packet buffers are owned values. Ownership transfer is part of the API.

Receive starts with a backend-owned pool. The socket fills mutable buffers and
pushes them into a caller-provided `RecvBatch`. Once delivered, the caller owns
those buffers until they are dropped or reused by backend-specific APIs.

Transmit starts with a mutable buffer allocated from the socket's transmit pool.
The caller writes packet bytes, freezes the buffer, wraps it in a transmit item,
and places it in a `TxSlot::Ready`. A successful send consumes accepted slots in
order by changing them to `TxSlot::Taken`.

Some backends need explicit completion draining. A zero-copy backend cannot
recycle a submitted transmit frame while the NIC may still read it. Those
backends use `drain_tx_completions()` after hardware or kernel completion.
Copy-based backends can return `Ok(0)`.

For AF_XDP, completion reclaim must preserve which UMEM region owned the frame.
TX-origin frames return to the TX pool, while RX-origin frames that were
reflected out through transmit return to the FILL path so receive capacity is
restored.

Live XDP packet buffers can move across threads, but they remain owned values,
not shared references. The socket and pools that created them must outlive every
outstanding buffer. If a live buffer is dropped on another thread, the frame
enters a bounded remote reclaim queue drained by the owner thread.

This model avoids hidden clones. If bytes move, the move is visible through a
buffer operation such as relocating prepend or append, or through a backend
boundary that copies, such as OS UDP receive into a pool-owned buffer.
