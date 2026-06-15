//! UDP network-tile contracts for `fast-socket-rs` sockets.
//!
//! This crate intentionally contains only the shared tile interface and common
//! packet/classifier types. Backend crates such as `fast-socket-udp-tile-os`
//! and `fast-socket-udp-tile-xdp` own the concrete queueing, batching, wakeup,
//! completion-drain, and worker-affinity policy.

#![deny(missing_docs)]

use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU16;
use std::sync::Arc;
use std::thread::JoinHandle;

use fast_socket_rs::{
    BufferAccessError, BufferLayout, EcnCodepoint, Error, PacketBuffer, PacketBufferMut,
    QueueAffinity, ReserveError, SendError, UdpRecvMeta, UdpRxBuffer, UdpSocket, UdpTransmit,
    UdpTxBuffer, UdpTxBufferMut,
};

/// Number of per-lane RX/TX queue slots used by default.
pub const DEFAULT_QUEUE_CAPACITY: usize = 1024;

/// Number of packets processed per socket per receive pass by default.
pub const DEFAULT_BATCH_SIZE: usize = 64;

/// Number of preallocated transmit buffers kept for lane threads by default.
pub const DEFAULT_TX_BUFFER_QUEUE_CAPACITY: usize = 1024;

/// Refill starts when the preallocated TX-buffer queue drops below this count.
pub const DEFAULT_TX_BUFFER_REFILL_WATERMARK: usize = 256;

/// Maximum TX buffers to allocate for one lane during one refill pass by
/// default.
pub const DEFAULT_TX_BUFFER_REFILL_BATCH: usize = 256;

/// Stable index of one socket inside a tile-owned socket set.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct SocketIndex(u16);

impl SocketIndex {
    /// Creates a socket index.
    #[must_use]
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    /// Returns the raw socket index.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }
}

impl From<u16> for SocketIndex {
    fn from(value: u16) -> Self {
        Self::new(value)
    }
}

/// Ingress classifier output.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IngressDecision {
    /// Deliver the packet to the lane RX queue at this index.
    Deliver(usize),
    /// Drop the packet.
    Drop,
}

/// Classifies one incoming packet into a lane RX queue or a drop decision.
pub trait IngressClassifier<M, B>: Send + Sync + 'static {
    /// Classifies `packet` and its metadata.
    fn classify(&self, meta: &M, packet: &B, rx_queue_count: usize) -> IngressDecision;
}

/// Classifier that maps every packet to RX queue 0.
#[derive(Clone, Copy, Debug, Default)]
pub struct AcceptAllClassifier;

impl<M, B> IngressClassifier<M, B> for AcceptAllClassifier {
    fn classify(&self, _meta: &M, _packet: &B, rx_queue_count: usize) -> IngressDecision {
        if rx_queue_count == 0 {
            IngressDecision::Drop
        } else {
            IngressDecision::Deliver(0)
        }
    }
}

/// Classifier that consistently maps a UDP source address to a lane queue.
#[derive(Clone, Copy, Debug, Default)]
pub struct SourceAddrClassifier;

impl<B> IngressClassifier<UdpRecvMeta, B> for SourceAddrClassifier {
    fn classify(&self, meta: &UdpRecvMeta, _packet: &B, rx_queue_count: usize) -> IngressDecision {
        if rx_queue_count == 0 {
            return IngressDecision::Drop;
        }
        let mut hash = match meta.source.ip() {
            IpAddr::V4(addr) => u32::from(addr) as u64,
            IpAddr::V6(addr) => {
                let segments = addr.segments();
                ((segments[0] as u64) << 48)
                    | ((segments[1] as u64) << 32)
                    | ((segments[4] as u64) << 16)
                    | (segments[5] as u64)
            }
        };
        hash ^= (meta.source.port() as u64) << 32;
        IngressDecision::Deliver((mix64(hash) as usize) % rx_queue_count)
    }
}

#[inline]
fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

/// A packet delivered from a network tile to one lane RX queue.
pub struct TileRxPacket<S: UdpSocket> {
    meta: S::RecvMeta,
    packet: UdpRxBuffer<S>,
    source_socket: SocketIndex,
}

impl<S: UdpSocket> TileRxPacket<S> {
    /// Creates a tile receive packet from backend-owned parts.
    ///
    /// # Safety
    ///
    /// `source_socket` must identify the socket in the owning tile that
    /// produced `packet`.
    #[must_use]
    pub unsafe fn new(
        meta: S::RecvMeta,
        packet: UdpRxBuffer<S>,
        source_socket: SocketIndex,
    ) -> Self {
        Self {
            meta,
            packet,
            source_socket,
        }
    }

    /// Returns the socket that produced this packet.
    #[must_use]
    pub const fn source_socket(&self) -> SocketIndex {
        self.source_socket
    }

    /// Borrows backend receive metadata.
    #[must_use]
    pub const fn meta(&self) -> &S::RecvMeta {
        &self.meta
    }

    /// Borrows the UDP payload buffer.
    #[must_use]
    pub const fn packet(&self) -> &UdpRxBuffer<S> {
        &self.packet
    }

    /// Splits this packet into its metadata, payload buffer, and source socket.
    #[must_use]
    pub fn into_parts(self) -> (S::RecvMeta, UdpRxBuffer<S>, SocketIndex) {
        (self.meta, self.packet, self.source_socket)
    }
}

impl<S> TileRxPacket<S>
where
    S: UdpSocket,
    UdpRxBuffer<S>: PacketBufferMut<Frozen = UdpTxBuffer<S>>,
{
    /// Converts the received packet into a transmit packet preserving the
    /// source socket index.
    #[must_use]
    pub fn into_transmit(self, destination: SocketAddr) -> TileTxPacket<S> {
        TileTxPacket::new(self.packet.freeze(), destination, self.source_socket)
    }
}

/// A batch of packets delivered from a network tile to one lane.
pub struct TileRxBatch<S: UdpSocket> {
    packets: Vec<TileRxPacket<S>>,
}

impl<S: UdpSocket> TileRxBatch<S> {
    /// Creates an empty receive batch with room for `capacity` packets.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            packets: Vec::with_capacity(capacity),
        }
    }

    /// Returns the number of packets in this batch.
    #[must_use]
    pub fn len(&self) -> usize {
        self.packets.len()
    }

    /// Returns `true` when this batch has no packets.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.packets.is_empty()
    }

    /// Returns the currently allocated packet capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.packets.capacity()
    }

    /// Reserves capacity for at least `additional` more packets.
    pub fn reserve(&mut self, additional: usize) {
        self.packets.reserve(additional);
    }

    /// Adds one packet to the end of this batch.
    pub fn push(&mut self, packet: TileRxPacket<S>) {
        self.packets.push(packet);
    }

    /// Removes the last packet from this batch.
    pub fn pop(&mut self) -> Option<TileRxPacket<S>> {
        self.packets.pop()
    }

    /// Drops every packet in this batch while keeping the allocation.
    pub fn clear(&mut self) {
        self.packets.clear();
    }

    /// Drains every packet in this batch while keeping the allocation.
    pub fn drain(&mut self) -> std::vec::Drain<'_, TileRxPacket<S>> {
        self.packets.drain(..)
    }
}

/// A mutable transmit buffer allocated by a tile-owned socket.
pub struct TileTxBuffer<S: UdpSocket> {
    source_socket: SocketIndex,
    buffer: UdpTxBufferMut<S>,
}

impl<S: UdpSocket> TileTxBuffer<S> {
    /// Creates a tile-owned transmit buffer wrapper.
    ///
    /// # Safety
    ///
    /// `source_socket` must identify the socket in the owning tile that
    /// allocated `buffer`.
    #[must_use]
    pub unsafe fn new(source_socket: SocketIndex, buffer: UdpTxBufferMut<S>) -> Self {
        Self {
            source_socket,
            buffer,
        }
    }

    /// Socket that allocated this buffer.
    ///
    /// Some backend implementations can later submit the frozen packet through
    /// another socket in the same tile when the backing memory is shareable.
    #[must_use]
    pub const fn source_socket(&self) -> SocketIndex {
        self.source_socket
    }

    /// Borrows the mutable packet buffer.
    #[must_use]
    pub const fn buffer(&self) -> &UdpTxBufferMut<S> {
        &self.buffer
    }

    /// Freezes this buffer into a tile transmit packet.
    #[must_use]
    pub fn freeze(self, destination: SocketAddr) -> TileTxPacket<S> {
        TileTxPacket::new(self.buffer.freeze(), destination, self.source_socket)
    }

    /// Splits this wrapper into its socket index and raw buffer.
    #[must_use]
    pub fn into_parts(self) -> (SocketIndex, UdpTxBufferMut<S>) {
        (self.source_socket, self.buffer)
    }

    /// Returns the total packet length.
    #[must_use]
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Returns `true` when the packet has no bytes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Returns public prefix space currently available before the packet.
    #[must_use]
    pub fn headroom(&self) -> usize {
        self.buffer.headroom()
    }

    /// Returns public suffix space currently available after the packet.
    #[must_use]
    pub fn tailroom(&self) -> usize {
        self.buffer.tailroom()
    }

    /// Returns the layout used to allocate this buffer.
    #[must_use]
    pub fn layout(&self) -> &BufferLayout {
        self.buffer.layout()
    }

    /// Iterates packet segments in packet-byte order.
    pub fn segments(&self) -> <UdpTxBufferMut<S> as PacketBuffer>::Segments<'_> {
        self.buffer.segments()
    }

    /// Reads exactly `dst.len()` bytes at `offset` across packet segments.
    pub fn read_at_exact(&self, offset: usize, dst: &mut [u8]) -> Result<(), BufferAccessError> {
        self.buffer.read_at_exact(offset, dst)
    }

    /// Prepends bytes immediately before the current packet start.
    pub fn prepend(&mut self, bytes: &[u8]) -> Result<(), ReserveError> {
        self.buffer.prepend(bytes)
    }

    /// Prepends bytes, relocating existing packet bytes when supported.
    pub fn prepend_relocating(&mut self, bytes: &[u8]) -> Result<(), ReserveError> {
        self.buffer.prepend_relocating(bytes)
    }

    /// Appends bytes to the packet tail.
    pub fn extend_from_slice(&mut self, bytes: &[u8]) -> Result<(), BufferAccessError> {
        self.buffer.extend_from_slice(bytes)
    }

    /// Appends bytes, relocating existing packet bytes when supported.
    pub fn extend_from_slice_relocating(&mut self, bytes: &[u8]) -> Result<(), BufferAccessError> {
        self.buffer.extend_from_slice_relocating(bytes)
    }

    /// Trims bytes from the packet prefix.
    pub fn trim_prefix(&mut self, len: usize) -> Result<(), BufferAccessError> {
        self.buffer.trim_prefix(len)
    }

    /// Trims bytes from the packet suffix.
    pub fn trim_suffix(&mut self, len: usize) -> Result<(), BufferAccessError> {
        self.buffer.trim_suffix(len)
    }
}

/// A packet queued by a lane for network transmit.
pub struct TileTxPacket<S: UdpSocket> {
    packet: UdpTxBuffer<S>,
    /// Remote destination address.
    pub destination: SocketAddr,
    source_socket: SocketIndex,
    /// Optional source IP selection.
    pub source_ip: Option<IpAddr>,
    /// Optional ECN codepoint.
    pub ecn: Option<EcnCodepoint>,
    /// Optional UDP segmentation size.
    pub gso_segment_size: Option<NonZeroU16>,
}

impl<S: UdpSocket> TileTxPacket<S> {
    const fn new(
        packet: UdpTxBuffer<S>,
        destination: SocketAddr,
        source_socket: SocketIndex,
    ) -> Self {
        Self {
            packet,
            destination,
            source_socket,
            source_ip: None,
            ecn: None,
            gso_segment_size: None,
        }
    }

    /// Borrows the UDP payload buffer.
    #[must_use]
    pub const fn packet(&self) -> &UdpTxBuffer<S> {
        &self.packet
    }

    /// Socket that produced or allocated the packet's backing buffer.
    ///
    /// Backends that cannot transfer buffers between sockets use this as the
    /// transmit socket. Backends with shareable backing memory may choose a
    /// different target socket at submit time.
    #[must_use]
    pub const fn source_socket(&self) -> SocketIndex {
        self.source_socket
    }

    /// Converts this tile packet into the core UDP transmit item.
    #[must_use]
    pub fn into_udp_transmit(self) -> UdpTransmit<UdpTxBuffer<S>> {
        UdpTransmit {
            packet: self.packet,
            destination: self.destination,
            source_ip: self.source_ip,
            ecn: self.ecn,
            gso_segment_size: self.gso_segment_size,
        }
    }
}

/// A batch of packets queued by a lane for network transmit.
pub struct TileTxBatch<S: UdpSocket> {
    packets: Vec<TileTxPacket<S>>,
}

impl<S: UdpSocket> TileTxBatch<S> {
    /// Creates an empty transmit batch with room for `capacity` packets.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            packets: Vec::with_capacity(capacity),
        }
    }

    /// Returns the number of packets in this batch.
    #[must_use]
    pub fn len(&self) -> usize {
        self.packets.len()
    }

    /// Returns `true` when this batch has no packets.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.packets.is_empty()
    }

    /// Returns the currently allocated packet capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.packets.capacity()
    }

    /// Reserves capacity for at least `additional` more packets.
    pub fn reserve(&mut self, additional: usize) {
        self.packets.reserve(additional);
    }

    /// Adds one packet to the end of this batch.
    pub fn push(&mut self, packet: TileTxPacket<S>) {
        self.packets.push(packet);
    }

    /// Removes the last packet from this batch.
    pub fn pop(&mut self) -> Option<TileTxPacket<S>> {
        self.packets.pop()
    }

    /// Iterates packets in this batch without consuming them.
    pub fn iter(&self) -> std::slice::Iter<'_, TileTxPacket<S>> {
        self.packets.iter()
    }

    /// Drops every packet in this batch while keeping the allocation.
    pub fn clear(&mut self) {
        self.packets.clear();
    }

    /// Drains every packet in this batch while keeping the allocation.
    pub fn drain(&mut self) -> std::vec::Drain<'_, TileTxPacket<S>> {
        self.packets.drain(..)
    }
}

/// Result of checking a tile socket set's worker affinity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TileAffinity {
    /// No socket reported a concrete affinity.
    Any,
    /// All concrete socket affinities were compatible with this affinity.
    Affinity(QueueAffinity),
}

impl TileAffinity {
    /// Converts this result to a queue-affinity hint.
    #[must_use]
    pub const fn as_queue_affinity(self) -> QueueAffinity {
        match self {
            Self::Any => QueueAffinity::Any,
            Self::Affinity(affinity) => affinity,
        }
    }
}

/// Runtime configuration shared by tile implementations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TileConfig {
    /// Target packet capacity for each per-lane RX and TX queue.
    ///
    /// Backend implementations that queue packets in batches may translate
    /// this into fewer queue slots sized for [`TileConfig::batch_size`].
    pub queue_capacity: usize,
    /// Receive batch size used for each socket pass.
    pub batch_size: usize,
    /// Capacity of each per-lane preallocated TX-buffer queue.
    pub tx_buffer_queue_capacity: usize,
    /// Refill threshold for each per-lane preallocated TX-buffer queue.
    pub tx_buffer_refill_watermark: usize,
    /// Maximum TX buffers allocated during one refill pass.
    pub tx_buffer_refill_batch: usize,
    /// Whether the tile should pin its worker thread when sockets report a
    /// concrete compatible affinity.
    pub pin_thread: bool,
}

impl Default for TileConfig {
    fn default() -> Self {
        Self {
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            batch_size: DEFAULT_BATCH_SIZE,
            tx_buffer_queue_capacity: DEFAULT_TX_BUFFER_QUEUE_CAPACITY,
            tx_buffer_refill_watermark: DEFAULT_TX_BUFFER_REFILL_WATERMARK,
            tx_buffer_refill_batch: DEFAULT_TX_BUFFER_REFILL_BATCH,
            pin_thread: true,
        }
    }
}

/// Tile runtime statistics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TileStats {
    /// Packets dropped because the ingress classifier returned
    /// [`IngressDecision::Drop`].
    pub classifier_drops: u64,
    /// Packets dropped because the selected RX queue was invalid or full.
    pub rx_queue_drops: u64,
    /// Transmit packets dropped because they referenced an invalid source
    /// socket.
    pub tx_drops: u64,
}

/// Errors returned by tile runtimes.
#[derive(Debug)]
pub enum TileError {
    /// `start` was called more than once.
    AlreadyStarted,
    /// The socket factory panicked or its mutex was poisoned.
    FactoryPoisoned,
    /// The socket set contains no sockets.
    EmptySocketSet,
    /// The socket set contains more sockets than [`SocketIndex`] can name.
    TooManySockets {
        /// Number of sockets returned by the factory.
        count: usize,
    },
    /// Sockets in one tile reported conflicting concrete affinities.
    IncompatibleAffinity {
        /// First concrete affinity observed.
        expected: QueueAffinity,
        /// Conflicting concrete affinity.
        found: QueueAffinity,
    },
    /// Thread pinning failed.
    Pin(std::io::Error),
    /// Worker thread spawning failed.
    Spawn(std::io::Error),
    /// A socket operation failed.
    Socket(Error),
    /// A send operation failed after accepting part of a batch.
    Send(SendError),
}

impl fmt::Display for TileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyStarted => f.write_str("UDP tile was already started"),
            Self::FactoryPoisoned => f.write_str("UDP tile socket factory mutex was poisoned"),
            Self::EmptySocketSet => f.write_str("UDP tile socket factory returned no sockets"),
            Self::TooManySockets { count } => {
                write!(f, "UDP tile has too many sockets for SocketIndex: {count}")
            }
            Self::IncompatibleAffinity { expected, found } => write!(
                f,
                "UDP tile sockets reported incompatible affinities: expected {expected:?}, found {found:?}"
            ),
            Self::Pin(error) => write!(f, "UDP tile thread pinning failed: {error}"),
            Self::Spawn(error) => write!(f, "UDP tile worker spawn failed: {error}"),
            Self::Socket(error) => write!(f, "UDP tile socket operation failed: {error}"),
            Self::Send(error) => write!(f, "UDP tile send failed: {error}"),
        }
    }
}

impl std::error::Error for TileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Pin(error) | Self::Spawn(error) => Some(error),
            Self::Socket(error) => Some(error),
            Self::Send(error) => Some(error),
            _ => None,
        }
    }
}

impl From<Error> for TileError {
    fn from(value: Error) -> Self {
        Self::Socket(value)
    }
}

/// Per-lane handle used by application code to exchange UDP work with a tile.
///
/// Handles are `Send` so they can move into lane threads. The trait does not
/// require `Sync`, which lets backend implementations encode single-consumer
/// lane ownership in the concrete handle type.
pub trait UdpNetworkTileHandle: Send + 'static {
    /// Concrete socket type driven by the tile.
    type Socket: UdpSocket;

    /// Returns the lane index owned by this handle.
    fn lane_index(&self) -> usize;

    /// Pops one received packet batch from this lane.
    fn pop_rx_batch(&self) -> Option<TileRxBatch<Self::Socket>>;

    /// Pushes one transmit packet batch from this lane.
    fn push_tx_batch(
        &self,
        batch: TileTxBatch<Self::Socket>,
    ) -> Result<(), TileTxBatch<Self::Socket>>;

    /// Pops up to `count` preallocated transmit buffers for this lane into
    /// `out`.
    fn alloc_tx_buffers(
        &mut self,
        count: usize,
        out: &mut Vec<TileTxBuffer<Self::Socket>>,
    ) -> usize;

    /// Allocates an empty receive batch container.
    fn alloc_rx_batch(&self) -> TileRxBatch<Self::Socket>;

    /// Recycles an empty or discarded receive batch container.
    fn recycle_rx_batch(&self, batch: TileRxBatch<Self::Socket>);

    /// Allocates an empty transmit batch container.
    fn alloc_tx_batch(&self) -> TileTxBatch<Self::Socket>;

    /// Recycles an empty or discarded transmit batch container.
    fn recycle_tx_batch(&self, batch: TileTxBatch<Self::Socket>);
}

/// Public UDP tile interface.
pub trait UdpNetworkTile: Send + Sync + 'static {
    /// Concrete socket type driven by this tile.
    type Socket: UdpSocket;

    /// Concrete per-lane handle type.
    type Handle: UdpNetworkTileHandle<Socket = Self::Socket>;

    /// Creates a handle for `lane_index`.
    ///
    /// Returns `None` when the lane index is outside the tile's configured
    /// lane range.
    fn lane_handle(self: Arc<Self>, lane_index: usize) -> Option<Self::Handle>;

    /// Returns a snapshot of tile drop counters.
    fn stats(&self) -> TileStats;

    /// Starts the tile worker thread.
    fn start(
        self: Arc<Self>,
        tile_index: usize,
    ) -> Result<JoinHandle<Result<(), TileError>>, TileError>;
}

/// Validates that sockets in one tile have compatible worker affinity.
pub fn validate_socket_affinity<S: UdpSocket>(sockets: &[S]) -> Result<TileAffinity, TileError> {
    let mut selected = None;
    for socket in sockets {
        let affinity = socket.worker_affinity();
        if matches!(affinity, QueueAffinity::Any) {
            continue;
        }
        match selected {
            None => selected = Some(affinity),
            Some(existing) if existing == affinity => {}
            Some(existing) => {
                return Err(TileError::IncompatibleAffinity {
                    expected: existing,
                    found: affinity,
                });
            }
        }
    }
    Ok(match selected {
        Some(affinity) => TileAffinity::Affinity(affinity),
        None => TileAffinity::Any,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use fast_socket_rs::{
        BufferLayout, BufferPool, BusyPollDriver, PacketBuffer, RecvBatch, ReserveError, Segment,
        Segments, SocketId, TxSlot, UdpReceive,
    };

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct TestBuf {
        bytes: Vec<u8>,
        layout: BufferLayout,
    }

    impl TestBuf {
        fn new() -> Self {
            Self {
                bytes: Vec::new(),
                layout: BufferLayout::new(2048),
            }
        }
    }

    impl PacketBuffer for TestBuf {
        type Segments<'a> = Segments<'a>;

        fn len(&self) -> usize {
            self.bytes.len()
        }

        fn headroom(&self) -> usize {
            0
        }

        fn tailroom(&self) -> usize {
            self.layout
                .payload_capacity()
                .saturating_sub(self.bytes.len())
        }

        fn layout(&self) -> &BufferLayout {
            &self.layout
        }

        fn segments(&self) -> Self::Segments<'_> {
            Some(self.bytes.as_slice() as Segment<'_>).into_iter()
        }

        fn read_at_exact(
            &self,
            offset: usize,
            dst: &mut [u8],
        ) -> Result<(), fast_socket_rs::BufferAccessError> {
            let end = offset + dst.len();
            if end > self.bytes.len() {
                return Err(fast_socket_rs::BufferAccessError::OutOfBounds {
                    offset,
                    len: dst.len(),
                    packet_len: self.bytes.len(),
                });
            }
            dst.copy_from_slice(&self.bytes[offset..end]);
            Ok(())
        }
    }

    impl PacketBufferMut for TestBuf {
        type Frozen = TestBuf;

        fn prepend(&mut self, bytes: &[u8]) -> Result<(), ReserveError> {
            let mut next = bytes.to_vec();
            next.extend_from_slice(&self.bytes);
            self.bytes = next;
            Ok(())
        }

        fn extend_from_slice(
            &mut self,
            bytes: &[u8],
        ) -> Result<(), fast_socket_rs::BufferAccessError> {
            self.bytes.extend_from_slice(bytes);
            Ok(())
        }

        fn trim_prefix(&mut self, len: usize) -> Result<(), fast_socket_rs::BufferAccessError> {
            self.bytes.drain(..len);
            Ok(())
        }

        fn trim_suffix(&mut self, len: usize) -> Result<(), fast_socket_rs::BufferAccessError> {
            let new_len = self.bytes.len() - len;
            self.bytes.truncate(new_len);
            Ok(())
        }

        fn freeze(self) -> Self::Frozen {
            self
        }
    }

    #[derive(Clone, Debug)]
    struct TestPool {
        layout: BufferLayout,
    }

    impl Default for TestPool {
        fn default() -> Self {
            Self {
                layout: BufferLayout::new(2048),
            }
        }
    }

    impl BufferPool for TestPool {
        type Buffer = TestBuf;

        fn layout(&self) -> &BufferLayout {
            &self.layout
        }

        fn allocate(&mut self) -> Option<Self::Buffer> {
            Some(TestBuf::new())
        }
    }

    struct TestSocket {
        affinity: QueueAffinity,
        rx_pool: TestPool,
        tx_pool: TestPool,
        driver: BusyPollDriver,
    }

    impl TestSocket {
        fn new(affinity: QueueAffinity) -> Self {
            Self {
                affinity,
                rx_pool: TestPool::default(),
                tx_pool: TestPool::default(),
                driver: BusyPollDriver::new(),
            }
        }
    }

    impl UdpSocket for TestSocket {
        type RxPool = TestPool;
        type TxPool = TestPool;
        type Driver = BusyPollDriver;
        type RecvMeta = UdpRecvMeta;

        fn socket_id(&self) -> SocketId {
            SocketId::new(0)
        }

        fn mtu(&self) -> usize {
            1500
        }

        fn worker_affinity(&self) -> QueueAffinity {
            self.affinity
        }

        fn rx_pool(&self) -> &Self::RxPool {
            &self.rx_pool
        }

        fn rx_pool_mut(&mut self) -> &mut Self::RxPool {
            &mut self.rx_pool
        }

        fn tx_pool(&self) -> &Self::TxPool {
            &self.tx_pool
        }

        fn tx_pool_mut(&mut self) -> &mut Self::TxPool {
            &mut self.tx_pool
        }

        fn driver(&self) -> &Self::Driver {
            &self.driver
        }

        fn driver_mut(&mut self) -> &mut Self::Driver {
            &mut self.driver
        }

        fn send(&mut self, batch: &mut [TxSlot<UdpTransmit<TestBuf>>]) -> Result<usize, SendError> {
            for slot in batch.iter_mut() {
                let _ = slot.take();
            }
            Ok(batch.len())
        }

        fn recv(
            &mut self,
            _out: &mut RecvBatch<UdpReceive<TestBuf, Self::RecvMeta>>,
        ) -> Result<usize, Error> {
            Ok(0)
        }

        fn drain_tx_completions(&mut self) -> Result<usize, Error> {
            Ok(0)
        }
    }

    #[test]
    fn source_addr_classifier_is_stable() {
        let meta = UdpRecvMeta {
            source: "192.0.2.1:4433".parse().unwrap(),
            destination: None,
            ecn: None,
            len: 0,
            gro_stride: None,
        };
        let left = SourceAddrClassifier.classify(&meta, &TestBuf::new(), 4);
        let right = SourceAddrClassifier.classify(&meta, &TestBuf::new(), 4);
        assert_eq!(left, right);
    }

    #[test]
    fn affinity_accepts_any_and_matching_concrete() {
        let sockets = [
            TestSocket::new(QueueAffinity::Any),
            TestSocket::new(QueueAffinity::Core(7)),
            TestSocket::new(QueueAffinity::Core(7)),
        ];
        assert_eq!(
            validate_socket_affinity(&sockets).unwrap(),
            TileAffinity::Affinity(QueueAffinity::Core(7))
        );
    }

    #[test]
    fn affinity_rejects_conflicting_concrete_hints() {
        let sockets = [
            TestSocket::new(QueueAffinity::Core(7)),
            TestSocket::new(QueueAffinity::Core(8)),
        ];
        assert!(matches!(
            validate_socket_affinity(&sockets),
            Err(TileError::IncompatibleAffinity {
                expected: QueueAffinity::Core(7),
                found: QueueAffinity::Core(8),
            })
        ));
    }
}
