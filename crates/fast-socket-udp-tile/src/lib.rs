//! UDP network-tile orchestration for `fast-socket-rs` sockets.
//!
//! A tile owns one worker thread and one or more UDP sockets. It receives
//! packets from those sockets, classifies each ingress packet into a lane RX
//! queue, and drains lane TX queues back to the socket that owns each packet's
//! buffer.

#![deny(missing_docs)]

mod queue;

use std::fmt;
use std::marker::PhantomData;
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU16;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use crossbeam_queue::ArrayQueue;
use fast_socket_rs::{
    EcnCodepoint, Error, PacketBufferMut, QueueAffinity, RecvBatch, SendError, TxSlot, UdpReceive,
    UdpRecvMeta, UdpRxBuffer, UdpSocket, UdpTransmit, UdpTxBuffer, UdpTxBufferMut,
    pin_current_thread_to_affinity,
};

pub use queue::{Park, Queue, Spin, WaitStrategy, wait_any_non_empty};

/// Number of per-lane RX/TX queue slots used by default.
pub const DEFAULT_QUEUE_CAPACITY: usize = 1024;

/// Number of packets processed per socket per receive pass by default.
pub const DEFAULT_BATCH_SIZE: usize = 64;

/// Number of preallocated transmit buffers kept for lane threads by default.
pub const DEFAULT_TX_BUFFER_QUEUE_CAPACITY: usize = 1024;

/// Refill starts when the preallocated TX-buffer queue drops below this count.
pub const DEFAULT_TX_BUFFER_REFILL_WATERMARK: usize = 256;

/// Maximum TX buffers to allocate during one refill pass by default.
pub const DEFAULT_TX_BUFFER_REFILL_BATCH: usize = 64;

/// Shorthand for a tile-to-lane ingress queue.
pub type RxQueue<S, W> = Arc<Queue<TileRxPacket<S>, W>>;

/// Shorthand for a lane-to-tile egress queue.
pub type TxQueue<S, W> = Arc<Queue<TileTxPacket<S>, W>>;

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

    fn as_usize(self) -> usize {
        self.0 as usize
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
    /// Backend receive metadata.
    pub meta: S::RecvMeta,
    /// UDP payload buffer.
    pub packet: UdpRxBuffer<S>,
    /// Socket that produced this packet.
    pub source_socket: SocketIndex,
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

/// A mutable transmit buffer allocated by a tile-owned socket.
pub struct TileTxBuffer<S: UdpSocket> {
    source_socket: SocketIndex,
    buffer: UdpTxBufferMut<S>,
}

impl<S: UdpSocket> TileTxBuffer<S> {
    /// Creates a tile-owned transmit buffer wrapper.
    #[must_use]
    pub fn new(source_socket: SocketIndex, buffer: UdpTxBufferMut<S>) -> Self {
        Self {
            source_socket,
            buffer,
        }
    }

    /// Socket that allocated this buffer.
    #[must_use]
    pub const fn source_socket(&self) -> SocketIndex {
        self.source_socket
    }

    /// Borrows the mutable packet buffer.
    #[must_use]
    pub const fn buffer(&self) -> &UdpTxBufferMut<S> {
        &self.buffer
    }

    /// Mutably borrows the packet buffer.
    #[must_use]
    pub fn buffer_mut(&mut self) -> &mut UdpTxBufferMut<S> {
        &mut self.buffer
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
}

impl<S: UdpSocket> Deref for TileTxBuffer<S> {
    type Target = UdpTxBufferMut<S>;

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl<S: UdpSocket> DerefMut for TileTxBuffer<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer
    }
}

/// A packet queued by a lane for network transmit.
pub struct TileTxPacket<S: UdpSocket> {
    /// UDP payload buffer.
    pub packet: UdpTxBuffer<S>,
    /// Remote destination address.
    pub destination: SocketAddr,
    /// Socket that owns the packet's backing buffer.
    pub source_socket: SocketIndex,
    /// Optional source IP selection.
    pub source_ip: Option<IpAddr>,
    /// Optional ECN codepoint.
    pub ecn: Option<EcnCodepoint>,
    /// Optional UDP segmentation size.
    pub gso_segment_size: Option<NonZeroU16>,
}

impl<S: UdpSocket> TileTxPacket<S> {
    /// Creates a tile transmit packet for `destination`.
    #[must_use]
    pub const fn new(
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

    fn into_udp_transmit(self) -> UdpTransmit<UdpTxBuffer<S>> {
        UdpTransmit {
            packet: self.packet,
            destination: self.destination,
            source_ip: self.source_ip,
            ecn: self.ecn,
            gso_segment_size: self.gso_segment_size,
        }
    }
}

/// Mutable set of UDP sockets driven by one tile thread.
pub trait UdpSocketSet {
    /// Concrete socket type in this set.
    type Socket: UdpSocket;

    /// Runs set-local maintenance before each socket pass.
    ///
    /// Returns `true` when the maintenance work made observable progress.
    fn poll_maintenance(&mut self) -> bool {
        false
    }

    /// Returns all sockets in stable tile order.
    fn sockets_mut(&mut self) -> &mut [Self::Socket];
}

impl<S: UdpSocket> UdpSocketSet for Vec<S> {
    type Socket = S;

    fn sockets_mut(&mut self) -> &mut [Self::Socket] {
        self.as_mut_slice()
    }
}

impl<S: UdpSocket, const N: usize> UdpSocketSet for [S; N] {
    type Socket = S;

    fn sockets_mut(&mut self) -> &mut [Self::Socket] {
        self.as_mut_slice()
    }
}

#[cfg(feature = "xdp")]
impl<D, R> UdpSocketSet for fast_socket_xdp_rs::XdpUdpAggregate<D, R>
where
    D: fast_socket_rs::PollDriver,
    R: fast_socket_xdp_rs::XdpUdpRouter,
{
    type Socket = fast_socket_xdp_rs::XdpUdpSocket<D, R>;

    fn sockets_mut(&mut self) -> &mut [Self::Socket] {
        self.members_mut()
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
    fn as_queue_affinity(self) -> QueueAffinity {
        match self {
            Self::Any => QueueAffinity::Any,
            Self::Affinity(affinity) => affinity,
        }
    }
}

/// Runtime configuration for [`UdpTile`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TileConfig {
    /// Number of slots in each per-lane RX and TX queue.
    pub queue_capacity: usize,
    /// Receive batch size used for each socket pass.
    pub batch_size: usize,
    /// Capacity of the cross-thread preallocated TX-buffer queue.
    pub tx_buffer_queue_capacity: usize,
    /// Refill threshold for the preallocated TX-buffer queue.
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

/// Errors returned by the tile runtime.
#[derive(Debug)]
pub enum TileError {
    /// `start` was called more than once.
    AlreadyStarted,
    /// The socket factory panicked or its mutex was poisoned.
    FactoryPoisoned,
    /// The socket factory returned no sockets.
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

/// Public UDP tile interface.
pub trait UdpNetworkTile: Send + Sync + 'static {
    /// Concrete socket type driven by this tile.
    type Socket: UdpSocket;

    /// Queue wait strategy.
    type Wait: WaitStrategy;

    /// Pops up to `count` preallocated transmit buffers into `out`.
    fn alloc_tx_buffers(&self, count: usize, out: &mut Vec<TileTxBuffer<Self::Socket>>) -> usize;

    /// Returns tile-to-lane RX queues, one per lane.
    fn rx_queues(&self) -> &[RxQueue<Self::Socket, Self::Wait>];

    /// Returns lane-to-tile TX queues, one per lane.
    fn tx_queues(&self) -> &[TxQueue<Self::Socket, Self::Wait>];

    /// Returns a snapshot of tile drop counters.
    fn stats(&self) -> TileStats;

    /// Starts the tile worker thread.
    fn start(
        self: Arc<Self>,
        tile_index: usize,
    ) -> Result<JoinHandle<Result<(), TileError>>, TileError>;
}

/// Default UDP network tile implementation.
pub struct UdpTile<Set, W, C>
where
    Set: UdpSocketSet + 'static,
    W: WaitStrategy,
    <Set::Socket as UdpSocket>::RecvMeta: Send + 'static,
    C: IngressClassifier<<Set::Socket as UdpSocket>::RecvMeta, UdpRxBuffer<Set::Socket>>,
{
    socket_factory: Mutex<Option<Box<dyn FnOnce() -> Set + Send + 'static>>>,
    rx_queues: Vec<RxQueue<Set::Socket, W>>,
    tx_queues: Vec<TxQueue<Set::Socket, W>>,
    tx_buffer_queue: Arc<ArrayQueue<TileTxBuffer<Set::Socket>>>,
    classifier_drops: AtomicU64,
    rx_queue_drops: AtomicU64,
    tx_drops: AtomicU64,
    classifier: C,
    config: TileConfig,
    _marker: PhantomData<fn() -> (Set, W)>,
}

impl<Set, W, C> UdpTile<Set, W, C>
where
    Set: UdpSocketSet + 'static,
    W: WaitStrategy,
    <Set::Socket as UdpSocket>::RecvMeta: Send + 'static,
    C: IngressClassifier<<Set::Socket as UdpSocket>::RecvMeta, UdpRxBuffer<Set::Socket>>,
{
    /// Creates a tile with default configuration.
    #[must_use]
    pub fn new(
        factory: impl FnOnce() -> Set + Send + 'static,
        classifier: C,
        lane_count: usize,
    ) -> Self {
        Self::with_config(factory, classifier, lane_count, TileConfig::default())
    }

    /// Creates a tile with explicit configuration.
    #[must_use]
    pub fn with_config(
        factory: impl FnOnce() -> Set + Send + 'static,
        classifier: C,
        lane_count: usize,
        config: TileConfig,
    ) -> Self {
        assert!(lane_count > 0, "UdpTile requires at least one lane queue");
        assert!(
            config.batch_size > 0,
            "UdpTile batch_size must be at least one",
        );
        assert!(
            config.tx_buffer_queue_capacity > 0,
            "UdpTile tx_buffer_queue_capacity must be at least one",
        );
        assert!(
            config.tx_buffer_refill_batch > 0,
            "UdpTile tx_buffer_refill_batch must be at least one",
        );

        let rx_queues = (0..lane_count)
            .map(|_| Queue::<TileRxPacket<Set::Socket>, W>::new(config.queue_capacity))
            .collect();
        let tx_queues = (0..lane_count)
            .map(|_| Queue::<TileTxPacket<Set::Socket>, W>::new(config.queue_capacity))
            .collect();
        Self {
            socket_factory: Mutex::new(Some(Box::new(factory))),
            rx_queues,
            tx_queues,
            tx_buffer_queue: Arc::new(ArrayQueue::new(config.tx_buffer_queue_capacity)),
            classifier_drops: AtomicU64::new(0),
            rx_queue_drops: AtomicU64::new(0),
            tx_drops: AtomicU64::new(0),
            classifier,
            config,
            _marker: PhantomData,
        }
    }

    fn record_classifier_drop(&self) {
        self.classifier_drops.fetch_add(1, Ordering::Relaxed);
    }

    fn record_rx_queue_drop(&self) {
        self.rx_queue_drops.fetch_add(1, Ordering::Relaxed);
    }

    fn record_tx_drop(&self) {
        self.tx_drops.fetch_add(1, Ordering::Relaxed);
    }
}

impl<Set, W, C> UdpNetworkTile for UdpTile<Set, W, C>
where
    Set: UdpSocketSet + 'static,
    W: WaitStrategy,
    <Set::Socket as UdpSocket>::RecvMeta: Send + 'static,
    C: IngressClassifier<<Set::Socket as UdpSocket>::RecvMeta, UdpRxBuffer<Set::Socket>>,
{
    type Socket = Set::Socket;
    type Wait = W;

    fn alloc_tx_buffers(&self, count: usize, out: &mut Vec<TileTxBuffer<Self::Socket>>) -> usize {
        let mut allocated = 0usize;
        for _ in 0..count {
            let Some(buffer) = self.tx_buffer_queue.pop() else {
                break;
            };
            out.push(buffer);
            allocated += 1;
        }
        allocated
    }

    fn rx_queues(&self) -> &[RxQueue<Self::Socket, Self::Wait>] {
        &self.rx_queues
    }

    fn tx_queues(&self) -> &[TxQueue<Self::Socket, Self::Wait>] {
        &self.tx_queues
    }

    fn stats(&self) -> TileStats {
        TileStats {
            classifier_drops: self.classifier_drops.load(Ordering::Relaxed),
            rx_queue_drops: self.rx_queue_drops.load(Ordering::Relaxed),
            tx_drops: self.tx_drops.load(Ordering::Relaxed),
        }
    }

    fn start(
        self: Arc<Self>,
        tile_index: usize,
    ) -> Result<JoinHandle<Result<(), TileError>>, TileError> {
        let factory = self
            .socket_factory
            .lock()
            .map_err(|_| TileError::FactoryPoisoned)?
            .take()
            .ok_or(TileError::AlreadyStarted)?;

        thread::Builder::new()
            .name(format!("fastsock-udp-tile-{tile_index}"))
            .spawn(move || run_tile(self, factory(), tile_index))
            .map_err(TileError::Spawn)
    }
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

fn run_tile<Set, W, C>(
    tile: Arc<UdpTile<Set, W, C>>,
    mut socket_set: Set,
    _tile_index: usize,
) -> Result<(), TileError>
where
    Set: UdpSocketSet + 'static,
    W: WaitStrategy,
    <Set::Socket as UdpSocket>::RecvMeta: Send + 'static,
    C: IngressClassifier<<Set::Socket as UdpSocket>::RecvMeta, UdpRxBuffer<Set::Socket>>,
{
    let socket_count = {
        let sockets = socket_set.sockets_mut();
        if sockets.is_empty() {
            return Err(TileError::EmptySocketSet);
        }
        if sockets.len() > u16::MAX as usize + 1 {
            return Err(TileError::TooManySockets {
                count: sockets.len(),
            });
        }

        let affinity = validate_socket_affinity(sockets)?;
        if tile.config.pin_thread {
            let _ = pin_current_thread_to_affinity(affinity.as_queue_affinity())
                .map_err(TileError::Pin)?;
        }
        sockets.len()
    };

    for queue in &tile.tx_queues {
        queue.register_consumer();
    }

    let mut rx = RecvBatch::with_capacity(tile.config.batch_size);
    let mut tx_alloc_scratch = Vec::with_capacity(tile.config.tx_buffer_refill_batch);
    let mut pending_tx: Vec<Vec<TxSlot<UdpTransmit<UdpTxBuffer<Set::Socket>>>>> = (0..socket_count)
        .map(|_| Vec::with_capacity(tile.config.batch_size))
        .collect();

    loop {
        let mut progressed = false;

        progressed |= socket_set.poll_maintenance();
        progressed |= drain_lane_tx(&tile, &mut pending_tx);
        progressed |= flush_pending_tx(socket_set.sockets_mut(), &mut pending_tx)?;
        progressed |= drain_socket_completions(socket_set.sockets_mut())?;
        progressed |= refill_tx_buffers(&tile, socket_set.sockets_mut(), &mut tx_alloc_scratch)?;
        progressed |= recv_from_sockets(&tile, socket_set.sockets_mut(), &mut rx)?;

        if !progressed {
            wait_any_non_empty(&tile.tx_queues);
        }
    }
}

fn drain_lane_tx<Set, W, C>(
    tile: &UdpTile<Set, W, C>,
    pending_tx: &mut [Vec<TxSlot<UdpTransmit<UdpTxBuffer<Set::Socket>>>>],
) -> bool
where
    Set: UdpSocketSet + 'static,
    W: WaitStrategy,
    <Set::Socket as UdpSocket>::RecvMeta: Send + 'static,
    C: IngressClassifier<<Set::Socket as UdpSocket>::RecvMeta, UdpRxBuffer<Set::Socket>>,
{
    let mut progressed = false;
    for queue in &tile.tx_queues {
        while let Some(packet) = queue.pop() {
            progressed = true;
            let index = packet.source_socket.as_usize();
            let Some(bucket) = pending_tx.get_mut(index) else {
                tile.record_tx_drop();
                continue;
            };
            bucket.push(TxSlot::Ready(packet.into_udp_transmit()));
        }
    }
    progressed
}

fn flush_pending_tx<S>(
    sockets: &mut [S],
    pending_tx: &mut [Vec<TxSlot<UdpTransmit<UdpTxBuffer<S>>>>],
) -> Result<bool, TileError>
where
    S: UdpSocket,
{
    let mut progressed = false;
    for (socket, pending) in sockets.iter_mut().zip(pending_tx.iter_mut()) {
        while !pending.is_empty() {
            match socket.send(pending.as_mut_slice()) {
                Ok(0) => break,
                Ok(accepted) => {
                    pending.drain(..accepted);
                    progressed = true;
                    socket.notify_tx()?;
                }
                Err(error) => {
                    if error.accepted > 0 {
                        pending.drain(..error.accepted);
                    }
                    return Err(TileError::Send(error));
                }
            }
        }
    }
    Ok(progressed)
}

fn drain_socket_completions<S: UdpSocket>(sockets: &mut [S]) -> Result<bool, TileError> {
    let mut progressed = false;
    for socket in sockets {
        progressed |= socket.drain_tx_completions()? != 0;
    }
    Ok(progressed)
}

fn refill_tx_buffers<Set, W, C>(
    tile: &UdpTile<Set, W, C>,
    sockets: &mut [Set::Socket],
    scratch: &mut Vec<UdpTxBufferMut<Set::Socket>>,
) -> Result<bool, TileError>
where
    Set: UdpSocketSet + 'static,
    W: WaitStrategy,
    <Set::Socket as UdpSocket>::RecvMeta: Send + 'static,
    C: IngressClassifier<<Set::Socket as UdpSocket>::RecvMeta, UdpRxBuffer<Set::Socket>>,
{
    if tile.tx_buffer_queue.len() >= tile.config.tx_buffer_refill_watermark {
        return Ok(false);
    }

    let mut progressed = false;
    let per_socket = tile
        .config
        .tx_buffer_refill_batch
        .div_ceil(sockets.len())
        .max(1);
    for (index, socket) in sockets.iter_mut().enumerate() {
        if tile.tx_buffer_queue.len() >= tile.config.tx_buffer_queue_capacity {
            break;
        }
        scratch.clear();
        let allocated = socket.allocate_tx_batch(scratch, per_socket)?;
        progressed |= allocated != 0;
        let source_socket = SocketIndex::new(index as u16);
        for buffer in scratch.drain(..) {
            if tile
                .tx_buffer_queue
                .push(TileTxBuffer::new(source_socket, buffer))
                .is_err()
            {
                break;
            }
        }
    }
    Ok(progressed)
}

fn recv_from_sockets<Set, W, C>(
    tile: &UdpTile<Set, W, C>,
    sockets: &mut [Set::Socket],
    rx: &mut RecvBatch<UdpReceive<UdpRxBuffer<Set::Socket>, <Set::Socket as UdpSocket>::RecvMeta>>,
) -> Result<bool, TileError>
where
    Set: UdpSocketSet + 'static,
    W: WaitStrategy,
    <Set::Socket as UdpSocket>::RecvMeta: Send + 'static,
    C: IngressClassifier<<Set::Socket as UdpSocket>::RecvMeta, UdpRxBuffer<Set::Socket>>,
{
    let mut progressed = false;
    for (socket_index, socket) in sockets.iter_mut().enumerate() {
        rx.clear();
        let received = socket.recv(rx)?;
        if received == 0 {
            continue;
        }
        progressed = true;
        let source_socket = SocketIndex::new(socket_index as u16);
        for item in rx.drain() {
            match tile
                .classifier
                .classify(&item.meta, &item.packet, tile.rx_queues.len())
            {
                IngressDecision::Drop => tile.record_classifier_drop(),
                IngressDecision::Deliver(index) => {
                    let Some(queue) = tile.rx_queues.get(index) else {
                        tile.record_rx_queue_drop();
                        continue;
                    };
                    if queue
                        .push(TileRxPacket {
                            meta: item.meta,
                            packet: item.packet,
                            source_socket,
                        })
                        .is_err()
                    {
                        tile.record_rx_queue_drop();
                    }
                }
            }
        }
    }
    Ok(progressed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fast_socket_rs::{
        BufferLayout, BufferPool, BusyPollDriver, PacketBuffer, ReserveError, Segment, Segments,
        SocketId,
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
