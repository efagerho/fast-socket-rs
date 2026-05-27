# UdpSocket

`UdpSocket` is the transport-facing socket trait for UDP payload I/O.

The declaration below omits default function bodies:

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

Associated types keep buffer pools, polling mode, and receive metadata
statically known for each backend. Generic code monomorphizes around those
types instead of carrying trait objects or dynamic packet state in the hot path.

`queue_id()` identifies the socket's queue. `mtu()` gives the maximum UDP
payload length accepted by transmit. `capabilities()` defaults to no optional
UDP offloads, so backends opt in only to features they honor.

`UdpTransmit<B>` contains a payload packet, remote destination, and optional
source IP, ECN, and GSO segment-size hints. `UdpReceive<B, M>` contains a
payload packet and metadata. The default metadata, `UdpRecvMeta`, records the
remote source, optional local destination IP, optional ECN, payload length, and
optional GRO stride.

The main operations are batch-oriented:

- `allocate_tx_batch(&mut Vec<_>, max)` fills caller-owned scratch capacity
  with mutable transmit buffers, draining completions when needed.
- `send(&mut [TxSlot<UdpTransmit<_>>])` consumes accepted leading slots in
  order.
- `recv(&mut RecvBatch<UdpReceive<_, _>>)` fills caller-owned batch capacity.
- `drain_tx_completions()` reclaims transmit resources for backends with explicit
  completion queues.
- `notify_tx()` notifies the transmit path when a backend needs an explicit
  doorbell.

Pool accessors are explicit because packet allocation is queue-local socket
state, not a global allocator. `allocate_tx_batch()` allocates from the transmit
pool and drains completions once when the pool is empty. Backends with bulk
allocation paths can override it.

`send()` and `recv()` use caller-owned batch containers. This keeps scratch
allocation outside the packet loop and lets each backend map the operation to
its ring or syscall batching model. `send()` consumes accepted leading
`TxSlot::Ready` entries in order by changing them to `TxSlot::Taken`, making
ownership transfer visible.

`drain_tx_completions()` gives zero-copy backends a portable place to reclaim
frames after transmit completion. Copy-based sockets can make it a no-op.
`notify_tx()` is separate so a backend can batch descriptor production before
notifying transmit.

The OS backend implements `UdpSocket` directly. It uses readiness polling and
copy-based receive semantics. Its completion drain is an inlined no-op because
the kernel owns completion work.

The AF_XDP backend implements `UdpSocket` through `XdpUdpSocket`. It owns an
`XdpIpPacketSocket`, local IPv4 UDP address, and resolved egress handle, then
builds or parses Ethernet, IPv4, and UDP headers in the backend path.

The core crate has no generic UDP-over-IP adapter. Backends that expose UDP
implement `UdpSocket` directly, keeping capabilities, parsing, header
construction, and offload behavior statically known.

`UdpCapabilities` records transmit GSO, receive GRO, and an optional maximum GSO
segment count. Backends should report only capabilities the socket can honor.
For high-performance backends, capabilities should be statically known through
the socket type, associated types, or policy types so unsupported branches
optimize away.
