# Packet Processing

This chapter shows the shape of UDP worker loops. A loop receives packets into a
reused `RecvBatch`, prepares transmit buffers from the socket's transmit pool,
submits ready transmit slots, and then either waits for work or keeps
busy-polling.

The examples use `UdpSocket`. Additionally, `S` denotes a type that implements the
`UdpSocket` trait.

## Buffer Allocation

Hot loops should reuse batch containers. `RecvBatch` keeps its allocation between
iterations, and a transmit slot vector can keep unsent packets until the socket
accepts them.

The `allocate_tx_batch` helper allocates from the transmit pool and, if the pool
is empty, drains transmit completions once before retrying. That gives a send
loop a simple way to prepare new packet storage without embedding backend-specific
completion logic in the application.

```rust,ignore
use std::net::SocketAddr;

use fast_socket_rs::{
    Error, PacketBufferMut, TxSlot, UdpSocket, UdpTransmit, UdpTxBuffer,
    UdpTxBufferMut,
};

fn queue_payload<S>(
    socket: &mut S,
    scratch: &mut Vec<UdpTxBufferMut<S>>,
    pending: &mut Vec<TxSlot<UdpTransmit<UdpTxBuffer<S>>>>,
    destination: SocketAddr,
    payload: &[u8],
) -> Result<bool, Error>
where
    S: UdpSocket,
{
    scratch.clear();
    if socket.allocate_tx_batch(scratch, 1)? == 0 {
        return Ok(false);
    }

    let mut buffer = scratch
        .pop()
        .expect("allocate_tx_batch reported one allocated buffer");
    buffer
        .extend_from_slice(payload)
        .map_err(|_| Error::InvalidPacket)?;

    pending.push(UdpTransmit::new(buffer.freeze(), destination).into());
    Ok(true)
}
```

For payloads that need to be built in place, pass a closure that receives the
mutable transmit buffer. The closure only runs after a buffer has been allocated;
if the transmit pool is empty, the helper returns `Ok(false)` and leaves the
pending queue unchanged.

```rust,ignore
use std::net::SocketAddr;

use fast_socket_rs::{
    Error, PacketBufferMut, TxSlot, UdpSocket, UdpTransmit, UdpTxBuffer,
    UdpTxBufferMut,
};

fn queue_payload_with<S, F>(
    socket: &mut S,
    scratch: &mut Vec<UdpTxBufferMut<S>>,
    pending: &mut Vec<TxSlot<UdpTransmit<UdpTxBuffer<S>>>>,
    destination: SocketAddr,
    create_payload: F,
) -> Result<bool, Error>
where
    S: UdpSocket,
    F: FnOnce(&mut UdpTxBufferMut<S>) -> Result<(), Error>,
{
    scratch.clear();
    if socket.allocate_tx_batch(scratch, 1)? == 0 {
        return Ok(false);
    }

    let mut buffer = scratch
        .pop()
        .expect("allocate_tx_batch reported one allocated buffer");
    create_payload(&mut buffer)?;

    pending.push(UdpTransmit::new(buffer.freeze(), destination).into());
    Ok(true)
}

let queued = queue_payload_with(
    socket,
    &mut scratch,
    &mut pending,
    destination,
    |buffer| {
        buffer
            .extend_from_slice(b"payload built by the closure")
            .map_err(|_| Error::InvalidPacket)?;
        Ok(())
    },
)?;
```

The socket consumes accepted transmit slots in order. On success, the accepted
prefix has been marked `TxSlot::Taken`. On error, `SendError::accepted` tells the
caller how many leading slots were accepted before the failing slot. The
remaining tail still belongs to the caller and can be retried or dropped by
application policy.

```rust,ignore
use fast_socket_rs::{Error, TxSlot, UdpSocket, UdpTransmit, UdpTxBuffer};

fn flush_pending<S>(
    socket: &mut S,
    pending: &mut Vec<TxSlot<UdpTransmit<UdpTxBuffer<S>>>>,
) -> Result<bool, Error>
where
    S: UdpSocket,
{
    if pending.is_empty() {
        return Ok(false);
    }

    match socket.send(pending.as_mut_slice()) {
        Ok(accepted) => {
            pending.drain(..accepted);
            Ok(accepted != 0)
        }
        Err(error) => {
            pending.drain(..error.accepted);
            Err(error.kind)
        }
    }
}
```

For a simple burst where the caller wants to submit every slot before returning,
`send_all` wraps the same pattern and drains transmit completions after partial
acceptance. Worker loops usually keep the explicit `send` form so they can run
timers, handle receive work, or apply back-pressure policy between attempts.

## Wait-Driven Loops

A wait-driven socket uses a `PollDriver` that can wait for an external event
source. A typical worker tries receive, transmit, and completion work first. If
none of those steps makes progress, it waits on the driver.

```rust,ignore
use std::time::Duration;

use fast_socket_rs::{
    Error, PollDriver, RecvBatch, TxSlot, UdpSocket, UdpTransmit, UdpTxBuffer,
    WaitOutcome,
};

fn wait_driven_loop<S>(
    socket: &mut S,
    pending: &mut Vec<TxSlot<UdpTransmit<UdpTxBuffer<S>>>>,
) -> Result<(), Error>
where
    S: UdpSocket,
{
    let mut rx = RecvBatch::with_capacity(64);

    loop {
        let mut progressed = false;

        rx.clear();
        progressed |= socket.recv(&mut rx)? != 0;
        for packet in rx.drain() {
            // Process packet.packet and packet.meta here.
            let _ = packet;
        }

        // Application work may queue responses into `pending` here.

        progressed |= flush_pending(socket, pending)?;
        progressed |= socket.drain_tx_completions()? != 0;

        if !progressed {
            match socket
                .driver_mut()
                .wait(Some(Duration::from_millis(10)))?
            {
                WaitOutcome::Ready | WaitOutcome::Spurious => {}
                WaitOutcome::Timeout => {
                    // Run timers or control-plane work here.
                }
                _ => {}
            }
        }
    }
}
```

`WaitOutcome::Ready` means the loop should try the socket again. `Timeout` means
the worker reached its chosen idle deadline. `Spurious` is valid and should be
handled like a normal loop retry. Wait-driven drivers may also expose a borrowed
wake handle through `wake_handle` for integration with an external reactor.

## Busy-Poll Loops

A busy-poll socket is intended for a worker that owns CPU time and repeatedly
probes the socket. Its driver has `PollMode::BusyPoll`; calling `wait` does not
sleep and returns a spurious outcome. When no work is available, the loop should
spin briefly, run periodic maintenance, or yield according to the application's
latency policy.

```rust,ignore
use fast_socket_rs::{
    Error, RecvBatch, TxSlot, UdpSocket, UdpTransmit, UdpTxBuffer,
};

fn busy_poll_loop<S>(
    socket: &mut S,
    pending: &mut Vec<TxSlot<UdpTransmit<UdpTxBuffer<S>>>>,
) -> Result<(), Error>
where
    S: UdpSocket,
{
    let mut rx = RecvBatch::with_capacity(64);

    loop {
        let mut progressed = false;

        rx.clear();
        progressed |= socket.recv(&mut rx)? != 0;
        for packet in rx.drain() {
            // Process packet.packet and packet.meta here.
            let _ = packet;
        }

        // Application work may queue responses into `pending` here.

        progressed |= flush_pending(socket, pending)?;
        progressed |= socket.drain_tx_completions()? != 0;

        if !progressed {
            core::hint::spin_loop();
        }
    }
}
```

Busy-poll loops should keep their working set small and reuse all hot-path
storage. The socket's `worker_affinity` can be used as a hint for pinning the
worker near the queue or device that the backend expects.

## Selecting a Loop

Select the worker loop once when the socket is created by checking the socket
driver's compile-time mode:

```rust,ignore
use fast_socket_rs::{Error, PollDriver, PollMode, TxSlot, UdpSocket, UdpTransmit, UdpTxBuffer};

fn run_socket<S>(
    socket: &mut S,
    pending: &mut Vec<TxSlot<UdpTransmit<UdpTxBuffer<S>>>>,
) -> Result<(), Error>
where
    S: UdpSocket,
{
    match <S::Driver as PollDriver>::MODE {
        PollMode::WaitDriven => wait_driven_loop(socket, pending),
        PollMode::BusyPoll => busy_poll_loop(socket, pending),
        _ => unreachable!("unknown poll mode"),
    }
}
```

That branch happens outside the packet hot path. After startup, the chosen loop
runs directly and does not repeatedly inspect the mode.
