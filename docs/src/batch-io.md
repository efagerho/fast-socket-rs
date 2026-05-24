# Batch I/O

Fast Socket treats batch I/O as the normal socket shape, not as a special
optimization layered on top of scalar operations.

Transmit batches are slices of `TxSlot<T>`. A slot is either `Ready(T)` or
`Taken`. The caller submits ready slots. The backend consumes accepted slots by
taking ownership of each item and leaving `Taken` behind.

Send acceptance is prefix-ordered. If a backend accepts three packets from a
batch, those are the first three ready slots. It must not skip slot one and
accept slot two. This rule keeps retry logic simple and lets callers reuse the
unaccepted tail directly.

`send` has two partial outcomes:

- `Ok(n)` means `n` leading slots were accepted. If `n` is less than the batch
  length, the backend stopped cleanly, commonly because a ring or socket buffer
  is full.
- `Err(SendError { accepted, kind })` means `accepted` leading slots were
  consumed, and the next slot failed with `kind`.

Rejected slots and the untouched tail remain owned by the caller. Any future
adapter that temporarily converts a packet item must restore that conversion
before returning an unaccepted item.

Receive batches use `RecvBatch<T>`. The caller chooses capacity up front, the
socket pushes received items until capacity or backend availability is
exhausted, and the caller can inspect, drain, or clear the batch without
reallocating its storage.

The design keeps allocation policy outside the receive call. A backend may fail
to allocate receive buffers, run out of RX descriptors, or hit a transient
empty queue, but it should report those conditions through accepted count,
`WouldBlock`, or backend statistics rather than secretly growing unbounded
temporary storage.
