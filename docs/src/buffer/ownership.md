# Buffer Ownership

Packet buffers are owned values. Ownership transfer is part of the API surface,
not an implementation detail.

Receive starts with a backend-owned receive pool. The socket fills mutable
buffers and pushes them into a caller-provided `RecvBatch`. Once delivered, the
caller owns those packet buffers until they are dropped or reused by backend
specific APIs.

Transmit starts with a mutable buffer allocated from the socket's transmit pool.
The caller writes packet bytes, freezes the buffer, wraps it in a transmit item,
and places it in a `TxSlot::Ready`. A successful send consumes accepted slots in
order by changing them to `TxSlot::Taken`.

Some backends need explicit completion draining. A zero-copy backend cannot
immediately recycle a submitted transmit frame if the NIC may still read it.
Those backends use `drain_tx_completions()` to reclaim socket-owned buffers after
hardware or kernel completion. Copy-based backends can implement completion
drain as `Ok(0)`.

For AF_XDP, completion reclaim must preserve which UMEM region owned the frame.
TX-origin frames return to the TX pool, while RX-origin frames that were
reflected out through transmit return to the FILL path so receive capacity is
restored.

Live XDP packet buffers can move across threads, but they are still owned
values, not shared references. The socket and pools that created them must
outlive every outstanding buffer. If a live buffer is dropped on another thread,
the returned frame enters a bounded remote reclaim queue that the owner thread
drains before reusing frames.

This model avoids hidden clones. If packet bytes need to move, that should be
visible through a buffer operation such as relocating prepend or append, or
through a backend boundary that inherently copies, such as OS UDP receive into a
pool-owned buffer.
