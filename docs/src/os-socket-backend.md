# OS Socket Backend

The `fast-socket-os-rs` crate provides the direct operating-system UDP backend.
It implements the core `UdpSocket` traits on top of ordinary OS sockets while
keeping the packet API close to the shape used by faster backends: packets live
in pool-owned buffers, receive and transmit operate on batches, and the socket is
owned by one worker thread.

The core socket traits require socket pools to hand out `Send` buffers, including
the frozen transmit form. That contract applies to both `UdpSocket` and
`IpPacketSocket` implementations. The OS backend therefore allows packet
ownership to move across worker threads even though the live socket object itself
remains single-thread owned.

This backend is useful when an application wants the `fast-socket-rs` API without
requiring kernel bypass. It currently supports Linux, macOS, and FreeBSD. Linux
gets the most optimized path because it exposes `recvmmsg` and `sendmmsg`;
macOS and FreeBSD use portable UDP socket calls behind the same public traits.

## Copy Behavior

An OS UDP socket still copies packet bytes between kernel memory and user
memory. The backend does not change that fundamental cost. What it avoids is
extra copying after bytes have crossed the kernel boundary.

On receive, the backend allocates an `OsPacketBufMut` from the socket's receive
pool and gives the buffer's payload region directly to the OS:

- Linux stores the buffer pointer in an `iovec` and submits it through
  `recvmmsg`.
- macOS and FreeBSD pass the same payload storage to `recv_from`.

When a datagram arrives, the packet bytes are already in the buffer that is
returned to the application. The backend records the received length, attaches
metadata, and pushes the packet into the caller's `RecvBatch`; it does not copy
the payload into another packet object.

On transmit, the application fills an `OsPacketBufMut`, freezes it into
`OsPacketBuf`, and submits it in a `TxSlot`. Linux sends from the packet's
segment slice by building `iovec` entries that point at the existing packet
storage. macOS and FreeBSD use `send_to` with a slice of the same storage. The
backend consumes successfully sent slots so the underlying storage can return to
the pool when no longer referenced.

`OsPacketBuf` is single-segment today. That keeps the OS backend simple and maps
cleanly to UDP socket APIs. The core traits still expose packet segments, so code
written against the generic packet API can also work with backends that support
scatter/gather buffers.

## Reducing Syscalls

The Linux implementation batches UDP operations with `recvmmsg` and `sendmmsg`.
Each socket owns fixed syscall scratch arrays sized by
`OsUdpSocketConfig::max_batch`:

- receive source addresses;
- receive `iovec` entries;
- receive `mmsghdr` entries;
- receive control-message buffers for packet info;
- transmit destination addresses;
- transmit `mmsghdr` entries;
- transmit ranges into the temporary transmit `iovec` vector.

The default `max_batch` is `64`. That is large enough to amortize syscall
overhead for bursty UDP traffic while keeping the resident Linux syscall scratch
state to about 35 KiB before packet buffers and growable transmit iovecs. The
hard cap is `4096`, which bounds the fixed scratch arrays to about 2.2 MiB on
Linux before packet buffers.

The receive path asks the OS for up to the smaller of the caller's remaining
`RecvBatch` capacity and the socket's `max_batch`. The transmit path walks the
ready prefix of the caller's slots, validates MTU and unsupported options, then
sends chunks of at most `max_batch`.

Short sends and transient socket errors are reported as partial progress instead
of forcing the caller into a failed all-or-nothing operation. Invalid slots or
unsupported transmit metadata return a `SendError` with the number of packets
accepted before the error.

macOS and FreeBSD do not use the Linux multi-message syscalls, so they submit one
datagram per OS call. The API remains batched: callers can still pass a slice of
transmit slots or a receive batch, and the backend processes as much as the OS
path can make progress on without changing the application-facing code.

## Buffer Pools

Each `OsUdpSocket` owns separate receive and transmit `OsBufferPool` instances.
The pools are slab-backed. Packet buffers are `Send`, so an application can move
an owned buffer to another worker thread and let that thread drop it. To keep
the owner-thread path free of mutexes and per-buffer reference-count traffic,
each buffer holds raw pointers into pool-owned reclaim state. Owner-thread drops
return storage to a local free list; cross-thread drops enter a bounded MPSC
remote reclaim queue that the owner drains before reuse.

This relies on the core lifetime contract: the socket and its pools must outlive
every buffer they hand out. Debug builds, and release builds with the
`buffer-guard` feature enabled, check a lightweight owner-generation token on
buffer access and reclaim. Release builds without that feature compile those
checks away.

The pool stores reusable `Vec<u8>` allocations. When it needs more storage, it
grows by slabs of up to 64 backing buffers. Each backing allocation uses the
configured `BufferLayout`, including headroom and tailroom, but the OS backend
forces the layout to one packet segment.

Allocation follows a small fixed pattern:

1. Pop a backing buffer from the free list.
2. If the free list is empty, grow by one slab unless the pool is at its cap.
3. Return `None` when the cap is reached and no buffer is free.
4. Wrap the storage in `OsPacketBufMut` with `start` and `end` at the payload
   offset.

When an `OsPacketBufMut` or frozen `OsPacketBuf` is dropped, its backing storage
is returned to the same pool. If the drop happens on the socket owner thread,
the storage goes straight to the local free list. If the drop happens on another
thread, the storage goes through the remote reclaim queue. The live socket
itself is still single-thread owned and should be driven by one worker, but
buffer storage can be reclaimed from any thread as long as the socket outlives
those buffers.

Pool size is configurable independently for receive and transmit:

```rust,ignore
use std::net::{Ipv4Addr, SocketAddrV4};

use fast_socket_os_rs::OsUdpSocketBuilder;
use fast_socket_rs::BufferLayout;

let _socket = OsUdpSocketBuilder::new(
    SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0).into(),
)
.buffer_layout(BufferLayout::with_headroom_and_tailroom(2048, 64, 0))
.max_batch(128)
.rx_pool_max_buffers(1024)
.tx_pool_max_buffers(1024)
.bind()?;
# Ok::<_, std::io::Error>(())
```

If no explicit pool cap is set, each pool defaults to
`DEFAULT_POOL_BUFFER_MULTIPLIER * max_batch`. The default multiplier is `4`, so a
socket with the default batch size keeps at most 256 backing buffers per pool.
This makes retained packet memory proportional to the amount of batching the
socket is configured to use instead of relying on a large global cap.

## Metadata and Limitations

On Linux, the backend enables `IP_PKTINFO` and `IPV6_RECVPKTINFO` when available.
That lets receive metadata include the destination IP address the datagram landed
on. If packet-info support is unavailable, `UdpRecvMeta::destination` remains
`None`.

The OS backend reports unsupported transmit options rather than silently ignoring
them. Today it rejects source-IP selection, ECN setting, and GSO segment size on
transmit. It also rejects received datagrams that are larger than the configured
receive buffer payload capacity, and socket construction rejects receive layouts
whose payload capacity is smaller than the configured MTU.

Use the direct UDP benchmark to measure the backend on the current host:

```sh
cargo bench -p fast-socket-os-rs --bench direct_udp
```

The benchmark keeps a bounded number of loopback UDP datagrams in flight,
reuses caller-side receive and transmit batch storage, and measures the combined
send and receive path without Criterion scaffolding around the packet loop.
