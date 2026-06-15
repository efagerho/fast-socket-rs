//! Operating-system UDP network-tile orchestration.
//!
//! A tile owns one worker thread and one or more UDP sockets. It receives
//! packets from those sockets, classifies each ingress packet into a lane RX
//! queue, and drains lane TX queues back to the socket that owns each packet's
//! buffer.

#![deny(missing_docs)]

mod queue;

use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use crossbeam_queue::ArrayQueue;
use fast_socket_os_rs::OsUdpSocket;
use fast_socket_rs::{
    RecvBatch, TxSlot, UdpReceive, UdpRxBuffer, UdpSocket, UdpTransmit, UdpTxBuffer,
    UdpTxBufferMut, pin_current_thread_to_affinity,
};
use fast_socket_udp_tile::validate_socket_affinity;
pub use fast_socket_udp_tile::{
    AcceptAllClassifier, IngressClassifier, IngressDecision, SocketIndex, SourceAddrClassifier,
    TileConfig, TileError, TileRxBatch, TileRxPacket, TileRxQueue, TileStats, TileTxBatch,
    TileTxBuffer, TileTxPacket, TileTxQueue, UdpNetworkTile, UdpNetworkTileHandle,
};

pub use queue::{
    Park, Queue, Spin, SpscConsumer, SpscProducer, WaitStrategy, spsc_pair, wait_any_non_empty,
};

/// Shorthand for a tile-to-lane ingress queue.
pub type RxQueue<S, W> = Arc<Queue<TileRxBatch<S>, W>>;

/// Shorthand for a lane-to-tile egress queue.
pub type TxQueue<S, W> = Arc<Queue<TileTxBatch<S>, W>>;

/// Lane-owned handle for an [`UdpTile`].
///
/// The handle is `Send` so it can move into a producer or lane thread. It is
/// intentionally not `Sync`; each handle represents one lane's single-consumer
/// view of the tile queues and buffer pool.
pub struct UdpTileHandle<Set, W, C>
where
    Set: UdpSocketSet + 'static,
    W: WaitStrategy,
    <Set::Socket as UdpSocket>::RecvMeta: Send + 'static,
    C: IngressClassifier<<Set::Socket as UdpSocket>::RecvMeta, UdpRxBuffer<Set::Socket>>,
{
    tile: Arc<UdpTile<Set, W, C>>,
    tx_buffer_consumer: SpscConsumer<TileTxBuffer<Set::Socket>>,
    lane_index: usize,
    _not_sync: PhantomData<UnsafeCell<()>>,
}

impl<S, W> TileRxQueue<S> for Queue<TileRxBatch<S>, W>
where
    S: UdpSocket + 'static,
    S::RecvMeta: Send + 'static,
    W: WaitStrategy,
{
    fn pop_batch(&self) -> Option<TileRxBatch<S>> {
        Queue::pop(self)
    }
}

impl<S, W> TileTxQueue<S> for Queue<TileTxBatch<S>, W>
where
    S: UdpSocket + 'static,
    W: WaitStrategy,
{
    fn push_batch(&self, batch: TileTxBatch<S>) -> Result<(), TileTxBatch<S>> {
        Queue::push(self, batch)
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

/// OS-backed UDP tile over a vector of [`OsUdpSocket`] members.
pub type OsUdpTile<W, C> = UdpTile<Vec<OsUdpSocket>, W, C>;

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
    tx_buffer_producers: Mutex<Option<Vec<SpscProducer<TileTxBuffer<Set::Socket>>>>>,
    tx_buffer_consumers: Mutex<Vec<Option<SpscConsumer<TileTxBuffer<Set::Socket>>>>>,
    rx_batch_pool: Arc<ArrayQueue<TileRxBatch<Set::Socket>>>,
    tx_batch_pool: Arc<ArrayQueue<TileTxBatch<Set::Socket>>>,
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

        let rx_queue_capacity = rx_queue_capacity(config);
        let rx_queues = (0..lane_count)
            .map(|_| Queue::<TileRxBatch<Set::Socket>, W>::new(rx_queue_capacity))
            .collect();
        let tx_queue_capacity = tx_queue_capacity(config);
        let tx_queues = (0..lane_count)
            .map(|_| Queue::<TileTxBatch<Set::Socket>, W>::new(tx_queue_capacity))
            .collect();
        let (tx_buffer_producers, tx_buffer_consumers): (Vec<_>, Vec<_>) = (0..lane_count)
            .map(|_| {
                let (producer, consumer) = spsc_pair(config.tx_buffer_queue_capacity);
                (producer, Some(consumer))
            })
            .unzip();
        let rx_batch_pool_capacity = rx_queue_capacity
            .saturating_mul(lane_count)
            .saturating_add(lane_count)
            .max(1);
        let tx_batch_pool_capacity = tx_queue_capacity
            .saturating_mul(lane_count)
            .saturating_add(lane_count)
            .max(1);
        Self {
            socket_factory: Mutex::new(Some(Box::new(factory))),
            rx_queues,
            tx_queues,
            tx_buffer_producers: Mutex::new(Some(tx_buffer_producers)),
            tx_buffer_consumers: Mutex::new(tx_buffer_consumers),
            rx_batch_pool: Arc::new(ArrayQueue::new(rx_batch_pool_capacity)),
            tx_batch_pool: Arc::new(ArrayQueue::new(tx_batch_pool_capacity)),
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

    fn record_rx_queue_drops(&self, count: usize) {
        self.rx_queue_drops
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    fn record_tx_drop(&self) {
        self.tx_drops.fetch_add(1, Ordering::Relaxed);
    }

    fn recycle_rx_batch_container(&self, batch: TileRxBatch<Set::Socket>) {
        self.recycle_rx_batch(batch);
    }

    fn recycle_tx_batch_container(&self, batch: TileTxBatch<Set::Socket>) {
        self.recycle_tx_batch(batch);
    }
}

fn rx_queue_capacity(config: TileConfig) -> usize {
    config.queue_capacity.div_ceil(config.batch_size).max(1)
}

fn tx_queue_capacity(config: TileConfig) -> usize {
    config.queue_capacity.div_ceil(config.batch_size).max(1)
}

impl<Set, W, C> UdpNetworkTile for UdpTile<Set, W, C>
where
    Set: UdpSocketSet + 'static,
    W: WaitStrategy,
    <Set::Socket as UdpSocket>::RecvMeta: Send + 'static,
    C: IngressClassifier<<Set::Socket as UdpSocket>::RecvMeta, UdpRxBuffer<Set::Socket>>,
{
    type Socket = Set::Socket;
    type Handle = UdpTileHandle<Set, W, C>;
    type RxQueue = RxQueue<Set::Socket, W>;
    type TxQueue = TxQueue<Set::Socket, W>;

    fn lane_handle(self: Arc<Self>, lane_index: usize) -> Option<Self::Handle> {
        if lane_index >= self.rx_queues.len() {
            return None;
        }
        let tx_buffer_consumer = {
            let mut consumers = self.tx_buffer_consumers.lock().ok()?;
            consumers.get_mut(lane_index)?.take()?
        };
        Some(UdpTileHandle {
            tile: self,
            tx_buffer_consumer,
            lane_index,
            _not_sync: PhantomData,
        })
    }

    fn alloc_tx_buffers(
        &self,
        lane_index: usize,
        count: usize,
        out: &mut Vec<TileTxBuffer<Self::Socket>>,
    ) -> usize {
        let Ok(mut consumers) = self.tx_buffer_consumers.lock() else {
            return 0;
        };
        consumers
            .get_mut(lane_index)
            .and_then(Option::as_mut)
            .map_or(0, |consumer| consumer.pop_into(count, out))
    }

    fn alloc_rx_batch(&self) -> TileRxBatch<Self::Socket> {
        self.rx_batch_pool
            .pop()
            .unwrap_or_else(|| TileRxBatch::with_capacity(self.config.batch_size))
    }

    fn recycle_rx_batch(&self, mut batch: TileRxBatch<Self::Socket>) {
        batch.clear();
        if batch.capacity() < self.config.batch_size {
            batch.reserve(self.config.batch_size - batch.capacity());
        }
        let _ = self.rx_batch_pool.push(batch);
    }

    fn alloc_tx_batch(&self) -> TileTxBatch<Self::Socket> {
        self.tx_batch_pool
            .pop()
            .unwrap_or_else(|| TileTxBatch::with_capacity(self.config.batch_size))
    }

    fn recycle_tx_batch(&self, mut batch: TileTxBatch<Self::Socket>) {
        batch.clear();
        if batch.capacity() < self.config.batch_size {
            batch.reserve(self.config.batch_size - batch.capacity());
        }
        let _ = self.tx_batch_pool.push(batch);
    }

    fn rx_queues(&self) -> &[Self::RxQueue] {
        &self.rx_queues
    }

    fn tx_queues(&self) -> &[Self::TxQueue] {
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
        let tx_buffer_producers = self
            .tx_buffer_producers
            .lock()
            .map_err(|_| TileError::FactoryPoisoned)?
            .take()
            .ok_or(TileError::AlreadyStarted)?;

        thread::Builder::new()
            .name(format!("fastsock-udp-tile-{tile_index}"))
            .spawn(move || run_tile(self, factory(), tx_buffer_producers, tile_index))
            .map_err(TileError::Spawn)
    }
}

impl<Set, W, C> UdpNetworkTileHandle for UdpTileHandle<Set, W, C>
where
    Set: UdpSocketSet + 'static,
    W: WaitStrategy,
    <Set::Socket as UdpSocket>::RecvMeta: Send + 'static,
    C: IngressClassifier<<Set::Socket as UdpSocket>::RecvMeta, UdpRxBuffer<Set::Socket>>,
{
    type Socket = Set::Socket;

    fn lane_index(&self) -> usize {
        self.lane_index
    }

    fn pop_rx_batch(&self) -> Option<TileRxBatch<Self::Socket>> {
        self.tile.rx_queues[self.lane_index].pop_batch()
    }

    fn push_tx_batch(
        &self,
        batch: TileTxBatch<Self::Socket>,
    ) -> Result<(), TileTxBatch<Self::Socket>> {
        self.tile.tx_queues[self.lane_index].push_batch(batch)
    }

    fn alloc_tx_buffers(
        &mut self,
        count: usize,
        out: &mut Vec<TileTxBuffer<Self::Socket>>,
    ) -> usize {
        self.tx_buffer_consumer.pop_into(count, out)
    }

    fn alloc_rx_batch(&self) -> TileRxBatch<Self::Socket> {
        self.tile.alloc_rx_batch()
    }

    fn recycle_rx_batch(&self, batch: TileRxBatch<Self::Socket>) {
        self.tile.recycle_rx_batch(batch);
    }

    fn alloc_tx_batch(&self) -> TileTxBatch<Self::Socket> {
        self.tile.alloc_tx_batch()
    }

    fn recycle_tx_batch(&self, batch: TileTxBatch<Self::Socket>) {
        self.tile.recycle_tx_batch(batch);
    }
}

fn run_tile<Set, W, C>(
    tile: Arc<UdpTile<Set, W, C>>,
    mut socket_set: Set,
    mut tx_buffer_producers: Vec<SpscProducer<TileTxBuffer<Set::Socket>>>,
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
    let mut pending_rx = (0..tile.rx_queues.len())
        .map(|_| tile.alloc_rx_batch())
        .collect::<Vec<_>>();
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
        progressed |= refill_tx_buffers(
            &tile,
            &mut tx_buffer_producers,
            socket_set.sockets_mut(),
            &mut tx_alloc_scratch,
        )?;
        progressed |= recv_from_sockets(&tile, socket_set.sockets_mut(), &mut rx, &mut pending_rx)?;

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
        while let Some(mut batch) = queue.pop() {
            progressed = true;
            for packet in batch.drain() {
                let index = usize::from(packet.source_socket().get());
                let Some(bucket) = pending_tx.get_mut(index) else {
                    tile.record_tx_drop();
                    continue;
                };
                bucket.push(TxSlot::Ready(packet.into_udp_transmit()));
            }
            tile.recycle_tx_batch_container(batch);
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
    producers: &mut [SpscProducer<TileTxBuffer<Set::Socket>>],
    sockets: &mut [Set::Socket],
    scratch: &mut Vec<UdpTxBufferMut<Set::Socket>>,
) -> Result<bool, TileError>
where
    Set: UdpSocketSet + 'static,
    W: WaitStrategy,
    <Set::Socket as UdpSocket>::RecvMeta: Send + 'static,
    C: IngressClassifier<<Set::Socket as UdpSocket>::RecvMeta, UdpRxBuffer<Set::Socket>>,
{
    let mut progressed = false;
    for producer in producers {
        if producer.len() >= tile.config.tx_buffer_refill_watermark {
            continue;
        }
        let target = tile
            .config
            .tx_buffer_refill_batch
            .min(producer.remaining_capacity());
        if target == 0 {
            continue;
        }
        let per_socket = target.div_ceil(sockets.len()).max(1);
        let mut remaining = target;
        for (index, socket) in sockets.iter_mut().enumerate() {
            if remaining == 0 {
                break;
            }
            scratch.clear();
            let request = remaining.min(per_socket);
            let allocated = socket.allocate_tx_batch(scratch, request)?;
            progressed |= allocated != 0;
            let source_socket = SocketIndex::new(index as u16);
            for buffer in scratch.drain(..) {
                // SAFETY: this buffer was allocated from `socket`, whose
                // stable tile index is `source_socket`.
                let buffer = unsafe { TileTxBuffer::new(source_socket, buffer) };
                if producer.push(buffer).is_err() {
                    remaining = 0;
                    break;
                }
                remaining -= 1;
                if remaining == 0 {
                    break;
                }
            }
        }
    }
    Ok(progressed)
}

fn recv_from_sockets<Set, W, C>(
    tile: &UdpTile<Set, W, C>,
    sockets: &mut [Set::Socket],
    rx: &mut RecvBatch<UdpReceive<UdpRxBuffer<Set::Socket>, <Set::Socket as UdpSocket>::RecvMeta>>,
    pending_rx: &mut [TileRxBatch<Set::Socket>],
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
                    if index >= tile.rx_queues.len() {
                        tile.record_rx_queue_drop();
                        continue;
                    }
                    // SAFETY: this receive item came from `socket`, whose
                    // stable tile index is `source_socket`.
                    let packet =
                        unsafe { TileRxPacket::new(item.meta, item.packet, source_socket) };
                    pending_rx[index].push(packet);
                    if pending_rx[index].len() >= tile.config.batch_size {
                        flush_rx_batch(tile, index, pending_rx);
                    }
                }
            }
        }
    }
    for index in 0..pending_rx.len() {
        flush_rx_batch(tile, index, pending_rx);
    }
    Ok(progressed)
}

fn flush_rx_batch<Set, W, C>(
    tile: &UdpTile<Set, W, C>,
    index: usize,
    pending_rx: &mut [TileRxBatch<Set::Socket>],
) where
    Set: UdpSocketSet + 'static,
    W: WaitStrategy,
    <Set::Socket as UdpSocket>::RecvMeta: Send + 'static,
    C: IngressClassifier<<Set::Socket as UdpSocket>::RecvMeta, UdpRxBuffer<Set::Socket>>,
{
    if pending_rx[index].is_empty() {
        return;
    }

    let batch = mem::replace(&mut pending_rx[index], tile.alloc_rx_batch());
    let batch_len = batch.len();
    let Some(queue) = tile.rx_queues.get(index) else {
        tile.record_rx_queue_drops(batch_len);
        tile.recycle_rx_batch_container(batch);
        return;
    };

    if let Err(batch) = queue.push(batch) {
        tile.record_rx_queue_drops(batch_len);
        tile.recycle_rx_batch_container(batch);
    }
}
