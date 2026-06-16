//! Generic UDP tile runtime.

use std::cell::UnsafeCell;
use std::io;
use std::marker::PhantomData;
use std::mem;
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use fast_socket_rs::{
    PollDriver, RecvBatch, TxSlot, UdpReceive, UdpRxBuffer, UdpSocket, UdpTransmit, UdpTxBuffer,
    UdpTxBufferMut, pin_current_thread_to_affinity,
};

use crate::queue::{
    Queue, SpscConsumer, SpscProducer, TilePollMode, TilePollModeDriver, TilePollModeKind, Wake,
    spsc_pair,
};
use crate::{
    IngressClassifier, IngressDecision, SocketIndex, TileConfig, TileError, TileRxBatch,
    TileRxPacket, TileStats, TileTxBuffer, TileTxMeta, TileTxPacket, UdpNetworkTile,
    UdpNetworkTileHandle, validate_socket_affinity,
};

type RxQueue<S> = Arc<Queue<TileRxBatch<S>>>;

const PARK_IDLE_TIMEOUT_MS: i32 = 1;

struct TileTxBufferWork<S: UdpSocket> {
    buffer: TileTxBuffer<S>,
    meta: TileTxMeta,
}

impl<S: UdpSocket> TileTxBufferWork<S> {
    fn source_socket(&self) -> SocketIndex {
        self.buffer.source_socket()
    }

    fn into_udp_transmit(self) -> UdpTransmit<UdpTxBuffer<S>> {
        let meta = self.meta;
        let mut packet = self.buffer.freeze(meta.destination);
        packet.source_ip = meta.source_ip;
        packet.source_port = meta.source_port;
        packet.ecn = meta.ecn;
        packet.gso_segment_size = meta.gso_segment_size;
        packet.into_udp_transmit()
    }
}

/// Lane-owned handle for an [`UdpTile`].
///
/// The handle is `Send` so it can move into a producer or lane thread. It is
/// intentionally not `Sync`; each handle represents one lane's single-consumer
/// view of the tile queues and buffer pool.
pub struct UdpTileHandle<Set, W, C>
where
    Set: UdpSocketSet + 'static,
    W: TilePollMode,
    <Set::Socket as UdpSocket>::Driver: TilePollModeDriver<W>,
    <Set::Socket as UdpSocket>::RecvMeta: Send + 'static,
    C: IngressClassifier<<Set::Socket as UdpSocket>::RecvMeta, UdpRxBuffer<Set::Socket>>,
{
    tile: Arc<UdpTile<Set, W, C>>,
    tx_buffer_consumer: SpscConsumer<TileTxBuffer<Set::Socket>>,
    tx_buffer_work_producer: SpscProducer<TileTxBufferWork<Set::Socket>>,
    tx_packet_producer: SpscProducer<TileTxPacket<Set::Socket>>,
    lane_index: usize,
    _not_sync: PhantomData<UnsafeCell<()>>,
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

    /// Returns whether a transmit buffer from any member can be submitted
    /// through any other member's TX ring.
    fn can_transmit_from_any_socket(&self) -> bool {
        false
    }
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

/// Default UDP network tile implementation.
pub struct UdpTile<Set, W, C>
where
    Set: UdpSocketSet + 'static,
    W: TilePollMode,
    <Set::Socket as UdpSocket>::Driver: TilePollModeDriver<W>,
    <Set::Socket as UdpSocket>::RecvMeta: Send + 'static,
    C: IngressClassifier<<Set::Socket as UdpSocket>::RecvMeta, UdpRxBuffer<Set::Socket>>,
{
    socket_factory: Mutex<Option<Box<dyn FnOnce() -> Set + Send + 'static>>>,
    rx_queues: Vec<RxQueue<Set::Socket>>,
    tx_wakes: Vec<Arc<Wake<W>>>,
    tx_buffer_producers: Mutex<Option<Vec<SpscProducer<TileTxBuffer<Set::Socket>>>>>,
    tx_buffer_consumers: Mutex<Vec<Option<SpscConsumer<TileTxBuffer<Set::Socket>>>>>,
    tx_buffer_work_producers: Mutex<Vec<Option<SpscProducer<TileTxBufferWork<Set::Socket>>>>>,
    tx_buffer_work_consumers: Mutex<Option<Vec<SpscConsumer<TileTxBufferWork<Set::Socket>>>>>,
    tx_packet_producers: Mutex<Vec<Option<SpscProducer<TileTxPacket<Set::Socket>>>>>,
    tx_packet_consumers: Mutex<Option<Vec<SpscConsumer<TileTxPacket<Set::Socket>>>>>,
    rx_batch_pool: Arc<crossbeam_queue::ArrayQueue<TileRxBatch<Set::Socket>>>,
    classifier_drops: AtomicU64,
    rx_queue_drops: AtomicU64,
    tx_drops: AtomicU64,
    tx_packets: AtomicU64,
    classifier: C,
    config: TileConfig,
    _marker: PhantomData<fn() -> (Set, W)>,
}

impl<Set, W, C> UdpTile<Set, W, C>
where
    Set: UdpSocketSet + 'static,
    W: TilePollMode,
    <Set::Socket as UdpSocket>::Driver: TilePollModeDriver<W>,
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
            .map(|_| Queue::<TileRxBatch<Set::Socket>>::new(rx_queue_capacity))
            .collect();
        let tx_wakes = (0..lane_count).map(|_| Wake::<W>::new()).collect();
        let (tx_buffer_producers, tx_buffer_consumers): (Vec<_>, Vec<_>) = (0..lane_count)
            .map(|_| {
                let (producer, consumer) = spsc_pair(config.tx_buffer_queue_capacity);
                (producer, Some(consumer))
            })
            .unzip();
        let tx_work_capacity = config.queue_capacity.max(config.batch_size).max(1);
        let (tx_buffer_work_producers, tx_buffer_work_consumers): (Vec<_>, Vec<_>) = (0
            ..lane_count)
            .map(|_| {
                let (producer, consumer) = spsc_pair(tx_work_capacity);
                (Some(producer), consumer)
            })
            .unzip();
        let (tx_packet_producers, tx_packet_consumers): (Vec<_>, Vec<_>) = (0..lane_count)
            .map(|_| {
                let (producer, consumer) = spsc_pair(tx_work_capacity);
                (Some(producer), consumer)
            })
            .unzip();
        let rx_batch_pool_capacity = rx_queue_capacity
            .saturating_mul(lane_count)
            .saturating_add(lane_count)
            .max(1);
        Self {
            socket_factory: Mutex::new(Some(Box::new(factory))),
            rx_queues,
            tx_wakes,
            tx_buffer_producers: Mutex::new(Some(tx_buffer_producers)),
            tx_buffer_consumers: Mutex::new(tx_buffer_consumers),
            tx_buffer_work_producers: Mutex::new(tx_buffer_work_producers),
            tx_buffer_work_consumers: Mutex::new(Some(tx_buffer_work_consumers)),
            tx_packet_producers: Mutex::new(tx_packet_producers),
            tx_packet_consumers: Mutex::new(Some(tx_packet_consumers)),
            rx_batch_pool: Arc::new(crossbeam_queue::ArrayQueue::new(rx_batch_pool_capacity)),
            classifier_drops: AtomicU64::new(0),
            rx_queue_drops: AtomicU64::new(0),
            tx_drops: AtomicU64::new(0),
            tx_packets: AtomicU64::new(0),
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

    fn record_tx_packets(&self, count: usize) {
        self.tx_packets.fetch_add(count as u64, Ordering::Relaxed);
    }

    fn alloc_rx_batch(&self) -> TileRxBatch<Set::Socket> {
        self.rx_batch_pool
            .pop()
            .unwrap_or_else(|| TileRxBatch::with_capacity(self.config.batch_size))
    }

    fn recycle_rx_batch(&self, mut batch: TileRxBatch<Set::Socket>) {
        batch.clear();
        if batch.capacity() < self.config.batch_size {
            batch.reserve(self.config.batch_size - batch.capacity());
        }
        let _ = self.rx_batch_pool.push(batch);
    }
}

fn rx_queue_capacity(config: TileConfig) -> usize {
    config.queue_capacity.div_ceil(config.batch_size).max(1)
}

impl<Set, W, C> UdpNetworkTile for UdpTile<Set, W, C>
where
    Set: UdpSocketSet + 'static,
    W: TilePollMode,
    <Set::Socket as UdpSocket>::Driver: TilePollModeDriver<W>,
    <Set::Socket as UdpSocket>::RecvMeta: Send + 'static,
    C: IngressClassifier<<Set::Socket as UdpSocket>::RecvMeta, UdpRxBuffer<Set::Socket>>,
{
    type Socket = Set::Socket;
    type Handle = UdpTileHandle<Set, W, C>;

    fn lane_handle(self: Arc<Self>, lane_index: usize) -> Option<Self::Handle> {
        if lane_index >= self.rx_queues.len() {
            return None;
        }
        let tx_buffer_consumer = {
            let mut consumers = self.tx_buffer_consumers.lock().ok()?;
            consumers.get_mut(lane_index)?.take()?
        };
        let tx_buffer_work_producer = {
            let mut producers = self.tx_buffer_work_producers.lock().ok()?;
            producers.get_mut(lane_index)?.take()?
        };
        let tx_packet_producer = {
            let mut producers = self.tx_packet_producers.lock().ok()?;
            producers.get_mut(lane_index)?.take()?
        };
        Some(UdpTileHandle {
            tile: self,
            tx_buffer_consumer,
            tx_buffer_work_producer,
            tx_packet_producer,
            lane_index,
            _not_sync: PhantomData,
        })
    }

    fn stats(&self) -> TileStats {
        TileStats {
            classifier_drops: self.classifier_drops.load(Ordering::Relaxed),
            rx_queue_drops: self.rx_queue_drops.load(Ordering::Relaxed),
            tx_drops: self.tx_drops.load(Ordering::Relaxed),
            tx_packets: self.tx_packets.load(Ordering::Relaxed),
        }
    }

    fn start(
        self: Arc<Self>,
        tile_index: usize,
    ) -> Result<thread::JoinHandle<Result<(), TileError>>, TileError> {
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
        let tx_buffer_work_consumers = self
            .tx_buffer_work_consumers
            .lock()
            .map_err(|_| TileError::FactoryPoisoned)?
            .take()
            .ok_or(TileError::AlreadyStarted)?;
        let tx_packet_consumers = self
            .tx_packet_consumers
            .lock()
            .map_err(|_| TileError::FactoryPoisoned)?
            .take()
            .ok_or(TileError::AlreadyStarted)?;

        thread::Builder::new()
            .name(format!("fastsock-udp-tile-{tile_index}"))
            .spawn(move || {
                run_tile(
                    self,
                    factory(),
                    tx_buffer_producers,
                    tx_buffer_work_consumers,
                    tx_packet_consumers,
                )
            })
            .map_err(TileError::Spawn)
    }
}

impl<Set, W, C> UdpNetworkTileHandle for UdpTileHandle<Set, W, C>
where
    Set: UdpSocketSet + 'static,
    W: TilePollMode,
    <Set::Socket as UdpSocket>::Driver: TilePollModeDriver<W>,
    <Set::Socket as UdpSocket>::RecvMeta: Send + 'static,
    C: IngressClassifier<<Set::Socket as UdpSocket>::RecvMeta, UdpRxBuffer<Set::Socket>>,
{
    type Socket = Set::Socket;

    fn lane_index(&self) -> usize {
        self.lane_index
    }

    fn pop_rx_batch(&self) -> Option<TileRxBatch<Self::Socket>> {
        self.tile.rx_queues[self.lane_index].pop()
    }

    fn push_tx_buffers(
        &mut self,
        buffers: &mut Vec<TileTxBuffer<Self::Socket>>,
        meta: TileTxMeta,
    ) -> usize {
        // SAFETY: the mapping closure only wraps the moved buffer with
        // copyable metadata and cannot panic.
        let accepted = unsafe {
            self.tx_buffer_work_producer
                .push_many_from(buffers, |buffer| TileTxBufferWork { buffer, meta })
        };
        if accepted != 0 {
            self.tile.tx_wakes[self.lane_index].notify();
        }
        accepted
    }

    fn push_tx_packets(&mut self, packets: &mut Vec<TileTxPacket<Self::Socket>>) -> usize {
        // SAFETY: the mapping closure only wraps the moved packet and cannot
        // panic.
        let accepted = unsafe {
            self.tx_packet_producer
                .push_many_from(packets, |packet| packet)
        };
        if accepted != 0 {
            self.tile.tx_wakes[self.lane_index].notify();
        }
        accepted
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
}

struct IdleWait<M: TilePollMode> {
    poll_fds: Vec<libc::pollfd>,
    lane_wake_count: usize,
    _marker: PhantomData<M>,
}

impl<M: TilePollMode> IdleWait<M> {
    fn new<S>(sockets: &[S], wakes: &[Arc<Wake<M>>]) -> Result<Self, TileError>
    where
        S: UdpSocket,
        S::Driver: TilePollModeDriver<M>,
    {
        let mut poll_fds = Vec::new();
        let mut lane_wake_count = 0usize;

        if matches!(M::KIND, TilePollModeKind::Park) {
            for wake in wakes {
                let fd = wake.wake_fd().ok_or(TileError::MissingWakeHandle)?;
                poll_fds.push(poll_fd(fd));
                lane_wake_count += 1;
            }

            for socket in sockets {
                let fd = socket
                    .driver()
                    .wake_handle()
                    .ok_or(TileError::MissingWakeHandle)?
                    .borrowed_fd()
                    .as_raw_fd();
                poll_fds.push(poll_fd(fd));
            }
        }

        Ok(Self {
            poll_fds,
            lane_wake_count,
            _marker: PhantomData,
        })
    }

    fn wait<F>(&mut self, lane_tx_queues_empty: F) -> Result<(), TileError>
    where
        F: FnOnce() -> bool,
    {
        match M::KIND {
            TilePollModeKind::Spin => {
                std::hint::spin_loop();
                Ok(())
            }
            TilePollModeKind::Park => {
                self.drain_lane_wakes();
                if lane_tx_queues_empty() {
                    self.poll()?;
                }
                self.drain_ready_lane_wakes();
                Ok(())
            }
        }
    }

    fn drain_lane_wakes(&self) {
        for poll_fd in self.poll_fds.iter().take(self.lane_wake_count) {
            drain_eventfd(poll_fd.fd);
        }
    }

    fn drain_ready_lane_wakes(&self) {
        for poll_fd in self.poll_fds.iter().take(self.lane_wake_count) {
            if poll_fd.revents & libc::POLLIN != 0 {
                drain_eventfd(poll_fd.fd);
            }
        }
    }

    fn poll(&mut self) -> Result<(), TileError> {
        if self.poll_fds.is_empty() {
            return Err(TileError::MissingWakeHandle);
        }
        let rc = unsafe {
            libc::poll(
                self.poll_fds.as_mut_ptr(),
                self.poll_fds
                    .len()
                    .try_into()
                    .expect("poll fd count fits libc::nfds_t"),
                PARK_IDLE_TIMEOUT_MS,
            )
        };
        if rc < 0 {
            Err(TileError::Wait(io::Error::last_os_error()))
        } else {
            Ok(())
        }
    }
}

fn poll_fd(fd: i32) -> libc::pollfd {
    libc::pollfd {
        fd,
        events: libc::POLLIN | libc::POLLERR | libc::POLLHUP,
        revents: 0,
    }
}

fn drain_eventfd(fd: i32) {
    loop {
        let mut value = 0u64;
        let rc = unsafe {
            libc::read(
                fd,
                std::ptr::addr_of_mut!(value).cast(),
                std::mem::size_of::<u64>(),
            )
        };
        if rc >= 0 {
            continue;
        }
        let error = io::Error::last_os_error();
        match error.raw_os_error() {
            Some(code) if code == libc::EAGAIN || code == libc::EWOULDBLOCK => break,
            _ => {
                debug_assert!(false, "failed to drain UDP tile eventfd: {error}");
                break;
            }
        }
    }
}

fn run_tile<Set, W, C>(
    tile: Arc<UdpTile<Set, W, C>>,
    mut socket_set: Set,
    mut tx_buffer_producers: Vec<SpscProducer<TileTxBuffer<Set::Socket>>>,
    mut tx_buffer_work_consumers: Vec<SpscConsumer<TileTxBufferWork<Set::Socket>>>,
    mut tx_packet_consumers: Vec<SpscConsumer<TileTxPacket<Set::Socket>>>,
) -> Result<(), TileError>
where
    Set: UdpSocketSet + 'static,
    W: TilePollMode,
    <Set::Socket as UdpSocket>::Driver: TilePollModeDriver<W>,
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

    let mut rx = RecvBatch::with_capacity(tile.config.batch_size);
    let mut pending_rx = (0..tile.rx_queues.len())
        .map(|_| tile.alloc_rx_batch())
        .collect::<Vec<_>>();
    let mut tx_alloc_scratch = Vec::with_capacity(tile.config.tx_buffer_refill_batch);
    let mut pending_tx: Vec<Vec<TxSlot<UdpTransmit<UdpTxBuffer<Set::Socket>>>>> = (0..socket_count)
        .map(|_| Vec::with_capacity(tile.config.batch_size))
        .collect();
    let mut tx_admission = TxAdmission::new(socket_count, tile.config);
    let mut deferred_tx_buffer_work = (0..tile.tx_wakes.len()).map(|_| None).collect::<Vec<_>>();
    let mut deferred_tx_packet = (0..tile.tx_wakes.len()).map(|_| None).collect::<Vec<_>>();
    let mut tx_buffer_work_scratch = Vec::with_capacity(tile.config.batch_size);
    let mut tx_packet_scratch = Vec::with_capacity(tile.config.batch_size);
    let mut next_tx_socket = 0usize;
    let mut idle_wait = IdleWait::<W>::new(socket_set.sockets_mut(), &tile.tx_wakes)?;

    loop {
        let mut progressed = false;

        progressed |= socket_set.poll_maintenance();
        progressed |= drain_socket_completions(socket_set.sockets_mut())?;
        progressed |= flush_pending_tx(&tile, socket_set.sockets_mut(), &mut pending_tx)?;
        tx_admission.reset(&pending_tx);
        progressed |= drain_lane_tx_buffer_work(
            &tile,
            &socket_set,
            &mut tx_buffer_work_consumers,
            &mut pending_tx,
            &mut deferred_tx_buffer_work,
            &mut tx_buffer_work_scratch,
            &mut tx_admission,
            &mut next_tx_socket,
        );
        progressed |= drain_lane_tx_packets(
            &tile,
            &socket_set,
            &mut tx_packet_consumers,
            &mut pending_tx,
            &mut deferred_tx_packet,
            &mut tx_packet_scratch,
            &mut tx_admission,
            &mut next_tx_socket,
        );
        progressed |= flush_pending_tx(&tile, socket_set.sockets_mut(), &mut pending_tx)?;
        progressed |= refill_tx_buffers(
            &tile,
            &mut tx_buffer_producers,
            socket_set.sockets_mut(),
            &mut tx_alloc_scratch,
        )?;
        progressed |= recv_from_sockets(&tile, socket_set.sockets_mut(), &mut rx, &mut pending_rx)?;

        if !progressed {
            idle_wait.wait(|| {
                tx_buffer_work_consumers.iter().all(SpscConsumer::is_empty)
                    && tx_packet_consumers.iter().all(SpscConsumer::is_empty)
            })?;
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn drain_lane_tx_buffer_work<Set, W, C>(
    tile: &UdpTile<Set, W, C>,
    socket_set: &Set,
    consumers: &mut [SpscConsumer<TileTxBufferWork<Set::Socket>>],
    pending_tx: &mut [Vec<TxSlot<UdpTransmit<UdpTxBuffer<Set::Socket>>>>],
    deferred_work: &mut [Option<TileTxBufferWork<Set::Socket>>],
    scratch: &mut Vec<TileTxBufferWork<Set::Socket>>,
    admission: &mut TxAdmission,
    next_tx_socket: &mut usize,
) -> bool
where
    Set: UdpSocketSet + 'static,
    W: TilePollMode,
    <Set::Socket as UdpSocket>::Driver: TilePollModeDriver<W>,
    <Set::Socket as UdpSocket>::RecvMeta: Send + 'static,
    C: IngressClassifier<<Set::Socket as UdpSocket>::RecvMeta, UdpRxBuffer<Set::Socket>>,
{
    let mut progressed = false;
    let can_retarget = socket_set.can_transmit_from_any_socket();
    for (consumer, deferred) in consumers.iter_mut().zip(deferred_work.iter_mut()) {
        if can_retarget {
            progressed |= drain_retargetable_tx_buffer_work_queue(
                tile,
                consumer,
                pending_tx,
                deferred,
                scratch,
                admission,
                next_tx_socket,
            );
        } else {
            progressed |= drain_source_local_tx_buffer_work_queue(
                tile, consumer, pending_tx, deferred, admission,
            );
        }
    }
    progressed
}

#[allow(clippy::too_many_arguments)]
fn drain_lane_tx_packets<Set, W, C>(
    tile: &UdpTile<Set, W, C>,
    socket_set: &Set,
    consumers: &mut [SpscConsumer<TileTxPacket<Set::Socket>>],
    pending_tx: &mut [Vec<TxSlot<UdpTransmit<UdpTxBuffer<Set::Socket>>>>],
    deferred_work: &mut [Option<TileTxPacket<Set::Socket>>],
    scratch: &mut Vec<TileTxPacket<Set::Socket>>,
    admission: &mut TxAdmission,
    next_tx_socket: &mut usize,
) -> bool
where
    Set: UdpSocketSet + 'static,
    W: TilePollMode,
    <Set::Socket as UdpSocket>::Driver: TilePollModeDriver<W>,
    <Set::Socket as UdpSocket>::RecvMeta: Send + 'static,
    C: IngressClassifier<<Set::Socket as UdpSocket>::RecvMeta, UdpRxBuffer<Set::Socket>>,
{
    let mut progressed = false;
    let can_retarget = socket_set.can_transmit_from_any_socket();
    for (consumer, deferred) in consumers.iter_mut().zip(deferred_work.iter_mut()) {
        if can_retarget {
            progressed |= drain_retargetable_tx_packet_queue(
                tile,
                consumer,
                pending_tx,
                deferred,
                scratch,
                admission,
                next_tx_socket,
            );
        } else {
            progressed |=
                drain_source_local_tx_packet_queue(tile, consumer, pending_tx, deferred, admission);
        }
    }
    progressed
}

fn drain_retargetable_tx_buffer_work_queue<Set, W, C>(
    tile: &UdpTile<Set, W, C>,
    consumer: &mut SpscConsumer<TileTxBufferWork<Set::Socket>>,
    pending_tx: &mut [Vec<TxSlot<UdpTransmit<UdpTxBuffer<Set::Socket>>>>],
    deferred: &mut Option<TileTxBufferWork<Set::Socket>>,
    scratch: &mut Vec<TileTxBufferWork<Set::Socket>>,
    admission: &TxAdmission,
    next_tx_socket: &mut usize,
) -> bool
where
    Set: UdpSocketSet + 'static,
    W: TilePollMode,
    <Set::Socket as UdpSocket>::Driver: TilePollModeDriver<W>,
    <Set::Socket as UdpSocket>::RecvMeta: Send + 'static,
    C: IngressClassifier<<Set::Socket as UdpSocket>::RecvMeta, UdpRxBuffer<Set::Socket>>,
{
    let mut progressed = false;
    while admission.has_capacity(pending_tx) {
        let Some(target) =
            choose_target_socket_with_admission(pending_tx, admission, 1, next_tx_socket)
        else {
            break;
        };
        let capacity = admission.remaining_capacity(pending_tx, target);
        if capacity == 0 {
            break;
        }

        scratch.clear();
        if let Some(work) = deferred.take() {
            scratch.push(work);
        }
        let pop_count = capacity
            .saturating_sub(scratch.len())
            .min(tile.config.batch_size.saturating_sub(scratch.len()));
        if pop_count != 0 {
            consumer.pop_into(pop_count, scratch);
        }
        if scratch.is_empty() {
            break;
        }

        let socket_count = pending_tx.len();
        let bucket = &mut pending_tx[target];
        for work in scratch.drain(..) {
            if usize::from(work.source_socket().get()) >= socket_count {
                tile.record_tx_drop();
                continue;
            }
            bucket.push(TxSlot::Ready(work.into_udp_transmit()));
        }
        progressed = true;
    }
    progressed
}

fn drain_retargetable_tx_packet_queue<Set, W, C>(
    tile: &UdpTile<Set, W, C>,
    consumer: &mut SpscConsumer<TileTxPacket<Set::Socket>>,
    pending_tx: &mut [Vec<TxSlot<UdpTransmit<UdpTxBuffer<Set::Socket>>>>],
    deferred: &mut Option<TileTxPacket<Set::Socket>>,
    scratch: &mut Vec<TileTxPacket<Set::Socket>>,
    admission: &TxAdmission,
    next_tx_socket: &mut usize,
) -> bool
where
    Set: UdpSocketSet + 'static,
    W: TilePollMode,
    <Set::Socket as UdpSocket>::Driver: TilePollModeDriver<W>,
    <Set::Socket as UdpSocket>::RecvMeta: Send + 'static,
    C: IngressClassifier<<Set::Socket as UdpSocket>::RecvMeta, UdpRxBuffer<Set::Socket>>,
{
    let mut progressed = false;
    while admission.has_capacity(pending_tx) {
        let Some(target) =
            choose_target_socket_with_admission(pending_tx, admission, 1, next_tx_socket)
        else {
            break;
        };
        let capacity = admission.remaining_capacity(pending_tx, target);
        if capacity == 0 {
            break;
        }

        scratch.clear();
        if let Some(packet) = deferred.take() {
            scratch.push(packet);
        }
        let pop_count = capacity
            .saturating_sub(scratch.len())
            .min(tile.config.batch_size.saturating_sub(scratch.len()));
        if pop_count != 0 {
            consumer.pop_into(pop_count, scratch);
        }
        if scratch.is_empty() {
            break;
        }

        let socket_count = pending_tx.len();
        let bucket = &mut pending_tx[target];
        for packet in scratch.drain(..) {
            if usize::from(packet.source_socket().get()) >= socket_count {
                tile.record_tx_drop();
                continue;
            }
            bucket.push(TxSlot::Ready(packet.into_udp_transmit()));
        }
        progressed = true;
    }
    progressed
}

fn drain_source_local_tx_buffer_work_queue<Set, W, C>(
    tile: &UdpTile<Set, W, C>,
    consumer: &mut SpscConsumer<TileTxBufferWork<Set::Socket>>,
    pending_tx: &mut [Vec<TxSlot<UdpTransmit<UdpTxBuffer<Set::Socket>>>>],
    deferred: &mut Option<TileTxBufferWork<Set::Socket>>,
    admission: &TxAdmission,
) -> bool
where
    Set: UdpSocketSet + 'static,
    W: TilePollMode,
    <Set::Socket as UdpSocket>::Driver: TilePollModeDriver<W>,
    <Set::Socket as UdpSocket>::RecvMeta: Send + 'static,
    C: IngressClassifier<<Set::Socket as UdpSocket>::RecvMeta, UdpRxBuffer<Set::Socket>>,
{
    let mut progressed = false;
    while admission.has_capacity(pending_tx) {
        let Some(work) = deferred.take().or_else(|| consumer.pop()) else {
            break;
        };

        match drain_source_local_tx_buffer_work(tile, work, pending_tx, admission) {
            Ok(()) => progressed = true,
            Err(work) => {
                *deferred = Some(work);
                break;
            }
        }
    }
    progressed
}

fn drain_source_local_tx_packet_queue<Set, W, C>(
    tile: &UdpTile<Set, W, C>,
    consumer: &mut SpscConsumer<TileTxPacket<Set::Socket>>,
    pending_tx: &mut [Vec<TxSlot<UdpTransmit<UdpTxBuffer<Set::Socket>>>>],
    deferred: &mut Option<TileTxPacket<Set::Socket>>,
    admission: &TxAdmission,
) -> bool
where
    Set: UdpSocketSet + 'static,
    W: TilePollMode,
    <Set::Socket as UdpSocket>::Driver: TilePollModeDriver<W>,
    <Set::Socket as UdpSocket>::RecvMeta: Send + 'static,
    C: IngressClassifier<<Set::Socket as UdpSocket>::RecvMeta, UdpRxBuffer<Set::Socket>>,
{
    let mut progressed = false;
    while admission.has_capacity(pending_tx) {
        let Some(packet) = deferred.take().or_else(|| consumer.pop()) else {
            break;
        };

        match drain_source_local_tx_packet(tile, packet, pending_tx, admission) {
            Ok(()) => progressed = true,
            Err(packet) => {
                *deferred = Some(packet);
                break;
            }
        }
    }
    progressed
}

fn drain_source_local_tx_buffer_work<Set, W, C>(
    tile: &UdpTile<Set, W, C>,
    work: TileTxBufferWork<Set::Socket>,
    pending_tx: &mut [Vec<TxSlot<UdpTransmit<UdpTxBuffer<Set::Socket>>>>],
    admission: &TxAdmission,
) -> Result<(), TileTxBufferWork<Set::Socket>>
where
    Set: UdpSocketSet + 'static,
    W: TilePollMode,
    <Set::Socket as UdpSocket>::Driver: TilePollModeDriver<W>,
    <Set::Socket as UdpSocket>::RecvMeta: Send + 'static,
    C: IngressClassifier<<Set::Socket as UdpSocket>::RecvMeta, UdpRxBuffer<Set::Socket>>,
{
    let index = usize::from(work.source_socket().get());
    if index >= pending_tx.len() {
        tile.record_tx_drop();
        return Ok(());
    }
    if !admission.can_admit(pending_tx, index, 1) {
        return Err(work);
    }
    pending_tx[index].push(TxSlot::Ready(work.into_udp_transmit()));
    Ok(())
}

fn drain_source_local_tx_packet<Set, W, C>(
    tile: &UdpTile<Set, W, C>,
    packet: TileTxPacket<Set::Socket>,
    pending_tx: &mut [Vec<TxSlot<UdpTransmit<UdpTxBuffer<Set::Socket>>>>],
    admission: &TxAdmission,
) -> Result<(), TileTxPacket<Set::Socket>>
where
    Set: UdpSocketSet + 'static,
    W: TilePollMode,
    <Set::Socket as UdpSocket>::Driver: TilePollModeDriver<W>,
    <Set::Socket as UdpSocket>::RecvMeta: Send + 'static,
    C: IngressClassifier<<Set::Socket as UdpSocket>::RecvMeta, UdpRxBuffer<Set::Socket>>,
{
    let index = usize::from(packet.source_socket().get());
    if index >= pending_tx.len() {
        tile.record_tx_drop();
        return Ok(());
    }
    if !admission.can_admit(pending_tx, index, 1) {
        return Err(packet);
    }
    pending_tx[index].push(TxSlot::Ready(packet.into_udp_transmit()));
    Ok(())
}

struct TxAdmission {
    open: Vec<bool>,
    capacity: usize,
}

impl TxAdmission {
    fn new(socket_count: usize, config: TileConfig) -> Self {
        Self {
            open: vec![false; socket_count],
            capacity: config.queue_capacity.max(config.batch_size).max(1),
        }
    }

    fn reset<T>(&mut self, pending_tx: &[Vec<T>]) {
        debug_assert_eq!(pending_tx.len(), self.open.len());
        for (pending, open) in pending_tx.iter().zip(self.open.iter_mut()) {
            *open = pending.is_empty();
        }
    }

    fn has_capacity<T>(&self, pending_tx: &[Vec<T>]) -> bool {
        debug_assert_eq!(pending_tx.len(), self.open.len());
        pending_tx
            .iter()
            .zip(self.open.iter())
            .any(|(pending, open)| *open && pending.len() < self.capacity)
    }

    fn can_admit<T>(&self, pending_tx: &[Vec<T>], index: usize, required: usize) -> bool {
        self.open[index]
            && (pending_tx[index].is_empty()
                || pending_tx[index].len().saturating_add(required) <= self.capacity)
    }

    fn remaining_capacity<T>(&self, pending_tx: &[Vec<T>], index: usize) -> usize {
        if self.open[index] {
            self.capacity.saturating_sub(pending_tx[index].len())
        } else {
            0
        }
    }
}

fn choose_target_socket_with_admission<T>(
    pending_tx: &[Vec<T>],
    admission: &TxAdmission,
    required: usize,
    next_tx_socket: &mut usize,
) -> Option<usize> {
    for _ in 0..pending_tx.len() {
        let target = choose_next_target_socket(pending_tx.len(), next_tx_socket);
        if admission.can_admit(pending_tx, target, required) {
            return Some(target);
        }
    }
    None
}

fn choose_next_target_socket(socket_count: usize, next_tx_socket: &mut usize) -> usize {
    let index = *next_tx_socket % socket_count;
    *next_tx_socket = (*next_tx_socket).wrapping_add(1);
    index
}

fn flush_pending_tx<Set, W, C>(
    tile: &UdpTile<Set, W, C>,
    sockets: &mut [Set::Socket],
    pending_tx: &mut [Vec<TxSlot<UdpTransmit<UdpTxBuffer<Set::Socket>>>>],
) -> Result<bool, TileError>
where
    Set: UdpSocketSet + 'static,
    W: TilePollMode,
    <Set::Socket as UdpSocket>::Driver: TilePollModeDriver<W>,
    <Set::Socket as UdpSocket>::RecvMeta: Send + 'static,
    C: IngressClassifier<<Set::Socket as UdpSocket>::RecvMeta, UdpRxBuffer<Set::Socket>>,
{
    let mut progressed = false;
    for (socket, pending) in sockets.iter_mut().zip(pending_tx.iter_mut()) {
        while !pending.is_empty() {
            match socket.send(pending.as_mut_slice()) {
                Ok(0) => break,
                Ok(accepted) => {
                    tile.record_tx_packets(accepted);
                    pending.drain(..accepted);
                    progressed = true;
                }
                Err(error) => {
                    if error.accepted > 0 {
                        tile.record_tx_packets(error.accepted);
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
        let completed = socket.drain_tx_completions()?;
        if completed != 0 {
            progressed = true;
        }
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
    W: TilePollMode,
    <Set::Socket as UdpSocket>::Driver: TilePollModeDriver<W>,
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
    W: TilePollMode,
    <Set::Socket as UdpSocket>::Driver: TilePollModeDriver<W>,
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
    W: TilePollMode,
    <Set::Socket as UdpSocket>::Driver: TilePollModeDriver<W>,
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
        tile.recycle_rx_batch(batch);
        return;
    };

    if let Err(batch) = queue.push(batch) {
        tile.record_rx_queue_drops(batch_len);
        tile.recycle_rx_batch(batch);
    }
}
