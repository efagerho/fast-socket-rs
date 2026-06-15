//! AF_XDP UDP network-tile orchestration.
//!
//! A tile owns one worker thread and one or more UDP sockets. It receives
//! packets from those sockets, classifies each ingress packet into a lane RX
//! queue, and drains lane TX queues back to the socket that owns each packet's
//! buffer.

#![deny(missing_docs)]

mod queue;

use std::cell::UnsafeCell;
use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::mem;
use std::net::SocketAddrV4;
use std::ops::RangeInclusive;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam_queue::ArrayQueue;
use fast_socket_rs::{
    BusyPollDriver, QueueId, RecvBatch, TxSlot, UdpReceive, UdpRxBuffer, UdpSocket, UdpTransmit,
    UdpTxBuffer, UdpTxBufferMut, pin_current_thread_to_affinity,
};
use fast_socket_udp_tile::validate_socket_affinity;
pub use fast_socket_udp_tile::{
    AcceptAllClassifier, IngressClassifier, IngressDecision, SocketIndex, SourceAddrClassifier,
    TileConfig, TileError, TileRxBatch, TileRxPacket, TileStats, TileTxBatch, TileTxBuffer,
    TileTxPacket, UdpNetworkTile, UdpNetworkTileHandle,
};
use fast_socket_xdp_rs::{
    InterfaceSelector, RouteSnapshot, XdpFactory, XdpFactoryBuilder, XdpQueueLocalRouter,
    XdpRouteMonitor, XdpRouteMonitorHandle, XdpUdpAggregate, XdpUdpSocket,
};

pub use queue::{Park, Spin};
use queue::{Queue, SpscConsumer, SpscProducer, WaitStrategy, spsc_pair, wait_any_non_empty};

type RxQueue<S, W> = Arc<Queue<TileRxBatch<S>, W>>;

type TxQueue<S, W> = Arc<Queue<TileTxBatch<S>, W>>;

type DefaultXdpSocket = XdpUdpSocket<BusyPollDriver, XdpQueueLocalRouter>;
type DefaultXdpAggregate = XdpUdpAggregate<BusyPollDriver, XdpQueueLocalRouter>;
type DefaultXdpRecvMeta = <DefaultXdpSocket as UdpSocket>::RecvMeta;
type DefaultXdpRxBuffer = UdpRxBuffer<DefaultXdpSocket>;
type DefaultXdpTile<C> = UdpTile<RouteMonitoredXdpAggregate, Spin, C>;
type DefaultXdpTileHandle<C> = UdpTileHandle<RouteMonitoredXdpAggregate, Spin, C>;

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

impl<D, R> UdpSocketSet for fast_socket_xdp_rs::XdpUdpAggregate<D, R>
where
    D: fast_socket_rs::PollDriver,
    R: fast_socket_xdp_rs::XdpUdpRouter,
{
    type Socket = fast_socket_xdp_rs::XdpUdpSocket<D, R>;

    fn sockets_mut(&mut self) -> &mut [Self::Socket] {
        self.members_mut()
    }

    fn can_transmit_from_any_socket(&self) -> bool {
        self.members_share_umem()
    }
}

struct RouteMonitoredXdpAggregate {
    aggregate: DefaultXdpAggregate,
    route_updates: Vec<XdpRouteMonitorHandle>,
}

impl UdpSocketSet for RouteMonitoredXdpAggregate {
    type Socket = DefaultXdpSocket;

    fn poll_maintenance(&mut self) -> bool {
        let mut updates = 0usize;
        for (socket, route_update) in self
            .aggregate
            .members_mut()
            .iter_mut()
            .zip(self.route_updates.iter_mut())
        {
            updates += route_update.apply_updates(socket.routes_mut());
        }
        updates != 0
    }

    fn sockets_mut(&mut self) -> &mut [Self::Socket] {
        self.aggregate.members_mut()
    }

    fn can_transmit_from_any_socket(&self) -> bool {
        self.aggregate.members_share_umem()
    }
}

/// Builder for the common busy-poll AF_XDP UDP tile set.
///
/// The builder consumes an [`XdpFactory`] and hides the per-worker socket-set
/// wrapper, route-monitor handles, and concrete wait strategy from application
/// code. It returns an [`XdpUdpTiles`] value that can create lane handles and
/// monitor tile-worker failures.
pub struct XdpUdpTileBuilder<C = SourceAddrClassifier>
where
    C: IngressClassifier<DefaultXdpRecvMeta, DefaultXdpRxBuffer> + Clone,
{
    factory: XdpFactory,
    local: SocketAddrV4,
    lane_count: usize,
    classifier: C,
    config: TileConfig,
    route_poll_interval: Duration,
}

impl XdpUdpTileBuilder<SourceAddrClassifier> {
    /// Creates a builder using [`SourceAddrClassifier`] and default tile config.
    #[must_use]
    pub fn new(factory: XdpFactory, local: SocketAddrV4, lane_count: usize) -> Self {
        Self {
            factory,
            local,
            lane_count,
            classifier: SourceAddrClassifier,
            config: TileConfig::default(),
            route_poll_interval: Duration::from_secs(1),
        }
    }

    /// Creates a device-oriented builder for `device` and `local`.
    ///
    /// This is the high-level path for application code. It discovers the XDP
    /// queues, seeds routes from netlink, and installs a UDP destination-port
    /// filter for `local.port()`. Use [`XdpUdpTileDeviceBuilder::threads`] to
    /// choose the number of tile workers before calling
    /// [`XdpUdpTileDeviceBuilder::build`].
    pub fn bind_device(
        device: impl Into<String>,
        local: SocketAddrV4,
        lane_count: usize,
    ) -> io::Result<XdpUdpTileDeviceBuilder<SourceAddrClassifier>> {
        Self::bind_interface(InterfaceSelector::Name(device.into()), local, lane_count)
    }

    /// Creates a device-oriented builder for an interface selector and `local`.
    ///
    /// This is the same as [`Self::bind_device`], but accepts an
    /// [`InterfaceSelector`] for callers that already resolved an interface
    /// index.
    pub fn bind_interface(
        interface: InterfaceSelector,
        local: SocketAddrV4,
        lane_count: usize,
    ) -> io::Result<XdpUdpTileDeviceBuilder<SourceAddrClassifier>> {
        let factory = XdpFactoryBuilder::new(interface)?
            .udp_ports([local.port()])
            .route_snapshot(RouteSnapshot::from_netlink()?);
        Ok(XdpUdpTileDeviceBuilder {
            factory,
            local,
            lane_count,
            classifier: SourceAddrClassifier,
            config: TileConfig::default(),
            route_poll_interval: Duration::from_secs(1),
        })
    }
}

impl<C> XdpUdpTileBuilder<C>
where
    C: IngressClassifier<DefaultXdpRecvMeta, DefaultXdpRxBuffer> + Clone,
{
    /// Uses a different ingress classifier.
    #[must_use]
    pub fn classifier<N>(self, classifier: N) -> XdpUdpTileBuilder<N>
    where
        N: IngressClassifier<DefaultXdpRecvMeta, DefaultXdpRxBuffer> + Clone,
    {
        XdpUdpTileBuilder {
            factory: self.factory,
            local: self.local,
            lane_count: self.lane_count,
            classifier,
            config: self.config,
            route_poll_interval: self.route_poll_interval,
        }
    }

    /// Uses an explicit tile configuration.
    #[must_use]
    pub fn config(mut self, config: TileConfig) -> Self {
        self.config = config;
        self
    }

    /// Sets the netlink route-monitor polling interval.
    #[must_use]
    pub fn route_poll_interval(mut self, interval: Duration) -> Self {
        self.route_poll_interval = interval;
        self
    }

    /// Starts one tile worker per XDP factory worker plan.
    pub fn build(self) -> Result<XdpUdpTiles<C>, TileError> {
        let plans = self.factory.into_worker_plans();
        let monitor_queue = plans
            .first()
            .and_then(|plan| plan.queue_ids().first())
            .copied()
            .unwrap_or_else(|| QueueId::new(0));

        let mut route_monitor = XdpRouteMonitor::new();
        let mut workers = Vec::with_capacity(plans.len());
        for plan in plans {
            let route_updates = plan
                .queue_ids()
                .iter()
                .map(|_| route_monitor.register_queue())
                .collect::<Vec<_>>();
            workers.push((plan, route_updates));
        }
        let route_monitor_thread =
            route_monitor.start_netlink(monitor_queue, self.route_poll_interval);

        let mut tiles = Vec::with_capacity(workers.len());
        let mut worker_threads = Vec::with_capacity(workers.len());
        for (tile_index, (plan, route_updates)) in workers.into_iter().enumerate() {
            let local = self.local;
            let tile = Arc::new(DefaultXdpTile::with_config(
                move || RouteMonitoredXdpAggregate {
                    aggregate: plan
                        .open_udp_busy_poll(local)
                        .expect("failed to open XDP tile aggregate"),
                    route_updates,
                },
                self.classifier.clone(),
                self.lane_count,
                self.config,
            ));
            let handle = Arc::clone(&tile).start(tile_index)?;
            tiles.push(tile);
            worker_threads.push(Some(handle));
        }

        Ok(XdpUdpTiles {
            tiles,
            worker_threads,
            taken_lanes: vec![false; self.lane_count],
            _route_monitor_thread: route_monitor_thread,
        })
    }
}

/// Device-oriented builder for the common busy-poll AF_XDP UDP tile set.
///
/// This builder owns the lower-level [`XdpFactoryBuilder`] until
/// [`Self::build`], so application code can configure worker count and common
/// factory options without manually constructing an [`XdpFactory`].
pub struct XdpUdpTileDeviceBuilder<C = SourceAddrClassifier>
where
    C: IngressClassifier<DefaultXdpRecvMeta, DefaultXdpRxBuffer> + Clone,
{
    factory: XdpFactoryBuilder,
    local: SocketAddrV4,
    lane_count: usize,
    classifier: C,
    config: TileConfig,
    route_poll_interval: Duration,
}

impl<C> XdpUdpTileDeviceBuilder<C>
where
    C: IngressClassifier<DefaultXdpRecvMeta, DefaultXdpRxBuffer> + Clone,
{
    /// Sets the number of tile workers.
    ///
    /// The lower-level factory validates that this divides the claimed queue
    /// layout.
    #[must_use]
    pub fn threads(mut self, threads: usize) -> Self {
        self.factory = self.factory.threads(threads);
        self
    }

    /// Applies custom configuration to the underlying XDP factory builder.
    ///
    /// This keeps uncommon factory knobs available without making applications
    /// construct the whole factory by hand.
    #[must_use]
    pub fn configure_factory(
        mut self,
        configure: impl FnOnce(XdpFactoryBuilder) -> XdpFactoryBuilder,
    ) -> Self {
        self.factory = configure(self.factory);
        self
    }

    /// Redirects and accepts UDP traffic for the provided destination ports.
    ///
    /// The local address port remains the default source port for transmit
    /// packets; set [`TileTxPacket::source_port`] per packet when sending from a
    /// different bound port.
    #[must_use]
    pub fn udp_ports(mut self, ports: impl IntoIterator<Item = u16>) -> Self {
        self.factory = self.factory.udp_ports(ports);
        self
    }

    /// Redirects and accepts UDP traffic whose destination port is in `range`.
    ///
    /// The local address port remains the default source port for transmit
    /// packets; set [`TileTxPacket::source_port`] per packet when sending from a
    /// different bound port.
    #[must_use]
    pub fn udp_port_range(mut self, range: RangeInclusive<u16>) -> Self {
        self.factory = self.factory.udp_port_range(*range.start(), *range.end());
        self
    }

    /// Uses a different ingress classifier.
    #[must_use]
    pub fn classifier<N>(self, classifier: N) -> XdpUdpTileDeviceBuilder<N>
    where
        N: IngressClassifier<DefaultXdpRecvMeta, DefaultXdpRxBuffer> + Clone,
    {
        XdpUdpTileDeviceBuilder {
            factory: self.factory,
            local: self.local,
            lane_count: self.lane_count,
            classifier,
            config: self.config,
            route_poll_interval: self.route_poll_interval,
        }
    }

    /// Uses an explicit tile configuration.
    #[must_use]
    pub fn config(mut self, config: TileConfig) -> Self {
        self.config = config;
        self
    }

    /// Sets the netlink route-monitor polling interval.
    #[must_use]
    pub fn route_poll_interval(mut self, interval: Duration) -> Self {
        self.route_poll_interval = interval;
        self
    }

    /// Builds the XDP factory and starts one tile worker per worker plan.
    pub fn build(self) -> Result<XdpUdpTiles<C>, XdpTileBuildError> {
        let factory = self.factory.build().map_err(XdpTileBuildError::Setup)?;
        XdpUdpTileBuilder::new(factory, self.local, self.lane_count)
            .classifier(self.classifier)
            .config(self.config)
            .route_poll_interval(self.route_poll_interval)
            .build()
            .map_err(XdpTileBuildError::Tile)
    }
}

/// Error returned while building high-level XDP UDP tiles.
#[derive(Debug)]
pub enum XdpTileBuildError {
    /// XDP factory setup failed.
    Setup(io::Error),
    /// Tile startup failed.
    Tile(TileError),
}

impl fmt::Display for XdpTileBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Setup(error) => write!(f, "XDP tile setup failed: {error}"),
            Self::Tile(error) => write!(f, "XDP tile startup failed: {error}"),
        }
    }
}

impl std::error::Error for XdpTileBuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Setup(error) => Some(error),
            Self::Tile(error) => Some(error),
        }
    }
}

/// Started busy-poll AF_XDP UDP tiles.
pub struct XdpUdpTiles<C = SourceAddrClassifier>
where
    C: IngressClassifier<DefaultXdpRecvMeta, DefaultXdpRxBuffer> + Clone,
{
    tiles: Vec<Arc<DefaultXdpTile<C>>>,
    worker_threads: Vec<Option<JoinHandle<Result<(), TileError>>>>,
    taken_lanes: Vec<bool>,
    _route_monitor_thread: JoinHandle<()>,
}

impl<C> XdpUdpTiles<C>
where
    C: IngressClassifier<DefaultXdpRecvMeta, DefaultXdpRxBuffer> + Clone,
{
    /// Returns the number of tile workers.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tiles.len()
    }

    /// Returns `true` when no tile workers were started.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tiles.is_empty()
    }

    /// Creates one handle for `lane_index` on every tile.
    ///
    /// Returns `None` if the lane index is outside the configured lane range or
    /// if a handle for this lane was already taken.
    pub fn lane_handles(&mut self, lane_index: usize) -> Option<Vec<XdpUdpTileHandle<C>>> {
        if *self.taken_lanes.get(lane_index)? {
            return None;
        }

        let handles = self
            .tiles
            .iter()
            .map(|tile| {
                Arc::clone(tile)
                    .lane_handle(lane_index)
                    .map(|inner| XdpUdpTileHandle { inner })
            })
            .collect::<Option<Vec<_>>>()?;
        self.taken_lanes[lane_index] = true;
        Some(handles)
    }

    /// Returns summed drop counters across all tiles.
    #[must_use]
    pub fn stats(&self) -> TileStats {
        let mut total = TileStats::default();
        for tile in &self.tiles {
            let stats = tile.stats();
            total.classifier_drops += stats.classifier_drops;
            total.rx_queue_drops += stats.rx_queue_drops;
            total.tx_drops += stats.tx_drops;
            total.tx_packets += stats.tx_packets;
        }
        total
    }

    /// Checks whether any tile worker exited unexpectedly.
    pub fn check_worker_threads(&mut self) -> Result<(), TileWorkerError> {
        for (index, handle) in self.worker_threads.iter_mut().enumerate() {
            if !handle.as_ref().is_some_and(|handle| handle.is_finished()) {
                continue;
            }
            let handle = handle.take().expect("finished handle is present");
            match handle.join() {
                Ok(Ok(())) => return Err(TileWorkerError::Exited { index }),
                Ok(Err(error)) => return Err(TileWorkerError::Failed { index, error }),
                Err(_) => return Err(TileWorkerError::Panicked { index }),
            }
        }
        Ok(())
    }
}

/// Lane-owned handle for [`XdpUdpTiles`].
pub struct XdpUdpTileHandle<C = SourceAddrClassifier>
where
    C: IngressClassifier<DefaultXdpRecvMeta, DefaultXdpRxBuffer> + Clone,
{
    inner: DefaultXdpTileHandle<C>,
}

impl<C> UdpNetworkTileHandle for XdpUdpTileHandle<C>
where
    C: IngressClassifier<DefaultXdpRecvMeta, DefaultXdpRxBuffer> + Clone,
{
    type Socket = DefaultXdpSocket;

    fn lane_index(&self) -> usize {
        self.inner.lane_index()
    }

    fn pop_rx_batch(&self) -> Option<TileRxBatch<Self::Socket>> {
        self.inner.pop_rx_batch()
    }

    fn push_tx_batch(
        &self,
        batch: TileTxBatch<Self::Socket>,
    ) -> Result<(), TileTxBatch<Self::Socket>> {
        self.inner.push_tx_batch(batch)
    }

    fn alloc_tx_buffers(
        &mut self,
        count: usize,
        out: &mut Vec<TileTxBuffer<Self::Socket>>,
    ) -> usize {
        self.inner.alloc_tx_buffers(count, out)
    }

    fn alloc_rx_batch(&self) -> TileRxBatch<Self::Socket> {
        self.inner.alloc_rx_batch()
    }

    fn recycle_rx_batch(&self, batch: TileRxBatch<Self::Socket>) {
        self.inner.recycle_rx_batch(batch);
    }

    fn alloc_tx_batch(&self) -> TileTxBatch<Self::Socket> {
        self.inner.alloc_tx_batch()
    }

    fn recycle_tx_batch(&self, batch: TileTxBatch<Self::Socket>) {
        self.inner.recycle_tx_batch(batch);
    }
}

/// Failure reported by [`XdpUdpTiles::check_worker_threads`].
#[derive(Debug)]
pub enum TileWorkerError {
    /// A tile worker returned `Ok(())`, which should not happen for the
    /// long-running tile loop.
    Exited {
        /// Tile worker index.
        index: usize,
    },
    /// A tile worker returned a tile error.
    Failed {
        /// Tile worker index.
        index: usize,
        /// Worker error.
        error: TileError,
    },
    /// A tile worker panicked.
    Panicked {
        /// Tile worker index.
        index: usize,
    },
}

impl fmt::Display for TileWorkerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exited { index } => write!(f, "tile worker {index} exited unexpectedly"),
            Self::Failed { index, error } => write!(f, "tile worker {index} failed: {error}"),
            Self::Panicked { index } => write!(f, "tile worker {index} panicked"),
        }
    }
}

impl std::error::Error for TileWorkerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Failed { error, .. } => Some(error),
            _ => None,
        }
    }
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
    tx_buffer_producers: Mutex<Option<Vec<SpscProducer<TileTxBuffer<Set::Socket>>>>>,
    tx_buffer_consumers: Mutex<Vec<Option<SpscConsumer<TileTxBuffer<Set::Socket>>>>>,
    rx_batch_pool: Arc<ArrayQueue<TileRxBatch<Set::Socket>>>,
    tx_batch_pool: Arc<ArrayQueue<TileTxBatch<Set::Socket>>>,
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

    fn alloc_tx_batch(&self) -> TileTxBatch<Set::Socket> {
        self.tx_batch_pool
            .pop()
            .unwrap_or_else(|| TileTxBatch::with_capacity(self.config.batch_size))
    }

    fn recycle_tx_batch(&self, mut batch: TileTxBatch<Set::Socket>) {
        batch.clear();
        if batch.capacity() < self.config.batch_size {
            batch.reserve(self.config.batch_size - batch.capacity());
        }
        let _ = self.tx_batch_pool.push(batch);
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
        self.tile.rx_queues[self.lane_index].pop()
    }

    fn push_tx_batch(
        &self,
        batch: TileTxBatch<Self::Socket>,
    ) -> Result<(), TileTxBatch<Self::Socket>> {
        self.tile.tx_queues[self.lane_index].push(batch)
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
    let mut tx_admission = TxAdmission::new(socket_count, tile.config);
    let mut deferred_tx = (0..tile.tx_queues.len()).map(|_| None).collect::<Vec<_>>();
    let mut next_tx_socket = 0usize;

    loop {
        let mut progressed = false;

        progressed |= socket_set.poll_maintenance();
        progressed |= drain_socket_completions(socket_set.sockets_mut())?;
        progressed |= flush_pending_tx(&tile, socket_set.sockets_mut(), &mut pending_tx)?;
        tx_admission.reset(&pending_tx);
        progressed |= drain_lane_tx(
            &tile,
            &socket_set,
            &mut pending_tx,
            &mut deferred_tx,
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
            if has_pending_tx(&pending_tx) || has_deferred_tx(&deferred_tx) {
                W::do_wait();
            } else {
                wait_any_non_empty(&tile.tx_queues);
            }
        }
    }
}

enum TxDrainResult {
    Drained,
    NoCapacity,
}

fn drain_lane_tx<Set, W, C>(
    tile: &UdpTile<Set, W, C>,
    socket_set: &Set,
    pending_tx: &mut [Vec<TxSlot<UdpTransmit<UdpTxBuffer<Set::Socket>>>>],
    deferred_tx: &mut [Option<TileTxBatch<Set::Socket>>],
    admission: &mut TxAdmission,
    next_tx_socket: &mut usize,
) -> bool
where
    Set: UdpSocketSet + 'static,
    W: WaitStrategy,
    <Set::Socket as UdpSocket>::RecvMeta: Send + 'static,
    C: IngressClassifier<<Set::Socket as UdpSocket>::RecvMeta, UdpRxBuffer<Set::Socket>>,
{
    let mut progressed = false;
    let can_retarget = socket_set.can_transmit_from_any_socket();
    for (queue, deferred) in tile.tx_queues.iter().zip(deferred_tx.iter_mut()) {
        while admission.has_capacity(pending_tx) {
            let Some(mut batch) = deferred.take().or_else(|| queue.pop()) else {
                break;
            };

            let result = if can_retarget {
                drain_retargetable_tx_batch(tile, &mut batch, pending_tx, admission, next_tx_socket)
            } else {
                drain_source_local_tx_batch(tile, &mut batch, pending_tx, admission)
            };

            match result {
                TxDrainResult::Drained => {
                    progressed = true;
                    tile.recycle_tx_batch(batch);
                }
                TxDrainResult::NoCapacity => {
                    *deferred = Some(batch);
                    break;
                }
            }
        }
    }
    progressed
}

fn drain_retargetable_tx_batch<Set, W, C>(
    tile: &UdpTile<Set, W, C>,
    batch: &mut TileTxBatch<Set::Socket>,
    pending_tx: &mut [Vec<TxSlot<UdpTransmit<UdpTxBuffer<Set::Socket>>>>],
    admission: &TxAdmission,
    next_tx_socket: &mut usize,
) -> TxDrainResult
where
    Set: UdpSocketSet + 'static,
    W: WaitStrategy,
    <Set::Socket as UdpSocket>::RecvMeta: Send + 'static,
    C: IngressClassifier<<Set::Socket as UdpSocket>::RecvMeta, UdpRxBuffer<Set::Socket>>,
{
    let socket_count = pending_tx.len();
    let packet_count = batch.len();
    if packet_count == 0 {
        return TxDrainResult::Drained;
    }

    let Some(target) =
        choose_target_socket_with_admission(pending_tx, admission, packet_count, next_tx_socket)
    else {
        return TxDrainResult::NoCapacity;
    };
    let Some(bucket) = pending_tx.get_mut(target) else {
        tile.record_tx_drop();
        return TxDrainResult::Drained;
    };

    for packet in batch.drain() {
        if usize::from(packet.source_socket().get()) >= socket_count {
            tile.record_tx_drop();
            continue;
        }
        bucket.push(TxSlot::Ready(packet.into_udp_transmit()));
    }
    TxDrainResult::Drained
}

fn drain_source_local_tx_batch<Set, W, C>(
    tile: &UdpTile<Set, W, C>,
    batch: &mut TileTxBatch<Set::Socket>,
    pending_tx: &mut [Vec<TxSlot<UdpTransmit<UdpTxBuffer<Set::Socket>>>>],
    admission: &mut TxAdmission,
) -> TxDrainResult
where
    Set: UdpSocketSet + 'static,
    W: WaitStrategy,
    <Set::Socket as UdpSocket>::RecvMeta: Send + 'static,
    C: IngressClassifier<<Set::Socket as UdpSocket>::RecvMeta, UdpRxBuffer<Set::Socket>>,
{
    debug_assert_eq!(pending_tx.len(), admission.len());

    admission.clear_counts();
    for packet in batch.iter() {
        let index = usize::from(packet.source_socket().get());
        if index >= pending_tx.len() {
            continue;
        }
        let required = admission.add_count(index);
        if !admission.can_admit(pending_tx, index, required) {
            return TxDrainResult::NoCapacity;
        }
    }

    for packet in batch.drain() {
        let index = usize::from(packet.source_socket().get());
        let Some(bucket) = pending_tx.get_mut(index) else {
            tile.record_tx_drop();
            continue;
        };
        bucket.push(TxSlot::Ready(packet.into_udp_transmit()));
    }
    TxDrainResult::Drained
}

struct TxAdmission {
    open: Vec<bool>,
    counts: Vec<usize>,
    capacity: usize,
}

impl TxAdmission {
    fn new(socket_count: usize, config: TileConfig) -> Self {
        Self {
            open: vec![false; socket_count],
            counts: vec![0; socket_count],
            capacity: config.queue_capacity.max(config.batch_size).max(1),
        }
    }

    fn len(&self) -> usize {
        self.open.len()
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

    fn clear_counts(&mut self) {
        self.counts.fill(0);
    }

    fn add_count(&mut self, index: usize) -> usize {
        self.counts[index] += 1;
        self.counts[index]
    }

    fn can_admit<T>(&self, pending_tx: &[Vec<T>], index: usize, required: usize) -> bool {
        self.open[index]
            && (pending_tx[index].is_empty()
                || pending_tx[index].len().saturating_add(required) <= self.capacity)
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
    W: WaitStrategy,
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

fn has_pending_tx<T>(pending_tx: &[Vec<T>]) -> bool {
    pending_tx.iter().any(|pending| !pending.is_empty())
}

fn has_deferred_tx<S: UdpSocket>(deferred_tx: &[Option<TileTxBatch<S>>]) -> bool {
    deferred_tx.iter().any(Option::is_some)
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
        tile.recycle_rx_batch(batch);
        return;
    };

    if let Err(batch) = queue.push(batch) {
        tile.record_rx_queue_drops(batch_len);
        tile.recycle_rx_batch(batch);
    }
}
