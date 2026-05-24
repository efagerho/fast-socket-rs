# UdpSocket

`UdpSocket` is the transport-facing socket trait. It is the API callers should
prefer when they want UDP payload I/O rather than IP packet forwarding.

The declaration below shows the actual trait surface, with default function
bodies omitted:

```rust,ignore
pub trait UdpSocket {
    type RxPool: BufferPool;
    type TxPool: BufferPool;
    type Driver: PollDriver;
    type RecvMeta;

    fn queue_id(&self) -> QueueId;
    fn mtu(&self) -> usize;
    fn capabilities(&self) -> UdpCapabilities;

    fn rx_pool(&self) -> &Self::RxPool;
    fn rx_pool_mut(&mut self) -> &mut Self::RxPool;
    fn tx_pool(&self) -> &Self::TxPool;
    fn tx_pool_mut(&mut self) -> &mut Self::TxPool;

    fn allocate_tx_batch(
        &mut self,
        out: &mut Vec<UdpTxBufferMut<Self>>,
        max: usize,
    ) -> Result<usize, Error>
    where
        Self: Sized;

    fn driver(&self) -> &Self::Driver;
    fn driver_mut(&mut self) -> &mut Self::Driver;

    fn send(
        &mut self,
        batch: &mut [TxSlot<UdpTransmit<UdpTxBuffer<Self>>>],
    ) -> Result<usize, SendError>;

    fn recv(
        &mut self,
        out: &mut RecvBatch<UdpReceive<UdpRxBuffer<Self>, Self::RecvMeta>>,
    ) -> Result<usize, Error>;

    fn drain_tx_completions(&mut self) -> Result<usize, Error>;
    fn notify_tx(&mut self) -> Result<(), Error>;
}
```

The associated types define the concrete socket shape:

- `RxPool` allocates mutable receive buffers.
- `TxPool` allocates mutable transmit buffers that freeze before send.
- `Driver` selects readiness or busy-poll behavior.
- `RecvMeta` defines the metadata returned with each received packet.

The associated types are part of the performance design. They keep buffer pools,
polling mode, and receive metadata statically known for each concrete backend.
That lets generic code monomorphize around the real pool and metadata types
instead of carrying trait objects or dynamically typed packet state through the
hot path.

`queue_id()` identifies the queue owned by the socket, and `mtu()` gives the
maximum UDP payload length accepted by transmit. `capabilities()` defaults to
no optional UDP offloads, so backends opt in only to features they can actually
honor.

`UdpTransmit<B>` contains a payload packet, a remote destination address, and
optional source IP, ECN, and GSO segment-size hints. `UdpReceive<B, M>` contains
a payload packet and metadata. The default metadata, `UdpRecvMeta`, records the
remote source, optional local destination IP, optional ECN, payload length, and
optional GRO stride.

The main operations are batch-oriented:

- `allocate_tx_batch(&mut Vec<_>, max)` lets a socket fill caller-owned
  scratch capacity with mutable transmit buffers, draining completions when
  needed.
- `send(&mut [TxSlot<UdpTransmit<_>>])` consumes accepted leading slots in
  order.
- `recv(&mut RecvBatch<UdpReceive<_, _>>)` fills caller-owned batch capacity.
- `drain_tx_completions()` reclaims transmit resources for backends with explicit
  completion queues.
- `notify_tx()` notifies the transmit path when a backend needs an explicit
  doorbell.

The pool accessors are explicit because packet allocation is queue-local socket
state, not an ambient global allocator. `allocate_tx_batch()` has a default
implementation that allocates from the transmit pool and drains completions once
when the pool is empty, while backends with better bulk allocation paths can
override it.

`send()` and `recv()` operate on caller-owned batch containers. That keeps
scratch allocation outside the packet loop and gives each backend room to map
the operation to its natural ring or syscall batching model. `send()` consumes
accepted leading `TxSlot::Ready` entries in order by changing them to
`TxSlot::Taken`, so ownership transfer is visible to the caller.

`drain_tx_completions()` is required even though copy-based sockets do not need
it. The method gives zero-copy backends a portable place to reclaim transmitted
frames after the kernel or NIC is done with them. `notify_tx()` is separate so a
backend can batch descriptor production and notify the transmit path only when
needed.

The OS backend implements `UdpSocket` directly. It uses readiness polling and
copy-based OS receive semantics. Its completion drain is an inlined no-op
because the kernel owns any required completion work.

The AF_XDP backend also implements `UdpSocket` directly through `XdpUdpSocket`.
It owns an `XdpIpPacketSocket` plus local IPv4 UDP address and resolved egress
handle, then builds or parses Ethernet, IPv4, and UDP headers inside the
backend-specific path.

There is no built-in generic UDP-over-IP adapter in the current core crate.
Backends that expose UDP should implement `UdpSocket` directly, which keeps
capabilities, parsing, header construction, and offload behavior statically
known to the concrete backend.

`UdpCapabilities` is intentionally small. It currently records whether transmit
GSO and receive GRO are supported, plus an optional maximum GSO segment count.
Backends should report only capabilities that the socket can actually honor.
For high-performance backends, capabilities should be statically known whenever
possible through the concrete socket type, associated types, or policy types.
That lets the compiler optimize away unsupported feature branches instead of
checking capability flags in the steady-state packet path.
