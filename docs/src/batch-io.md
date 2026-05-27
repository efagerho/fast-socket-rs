# Batch I/O

Fast Socket treats batch I/O as the normal socket shape, not an optimization on
top of scalar operations.

Transmit batches are slices of `TxSlot<T>`. A slot is either `Ready(T)` or
`Taken`. The caller submits ready slots. The backend consumes accepted slots by
taking ownership of each item and leaving `Taken` behind.

Send acceptance is prefix-ordered. If a backend accepts three packets, they are
the first three ready slots. It must not skip slot one and accept slot two. This
keeps retry logic simple and leaves the unaccepted tail with the caller.

`send` has two partial outcomes:

- `Ok(n)` means `n` leading slots were accepted. If `n` is less than the batch
  length, the backend stopped cleanly, commonly because a ring or socket buffer
  is full.
- `Err(SendError { accepted, kind })` means `accepted` leading slots were
  consumed, and the next slot failed with `kind`.

Rejected slots and the untouched tail remain caller-owned. Any adapter that
temporarily converts a packet item must restore it before returning an
unaccepted item.

Receive batches use `RecvBatch<T>`. The caller chooses capacity up front. The
socket pushes received items until capacity or backend availability is
exhausted. The caller can inspect, drain, or clear the batch without
reallocation.

Allocation policy stays outside the receive call. A backend may fail to
allocate receive buffers, run out of RX descriptors, or hit an empty queue. It
reports those conditions through accepted count, `WouldBlock`, or backend
statistics instead of growing unbounded temporary storage.
