//! AF_XDP UDP network-tile orchestration.
//!
//! This crate provides AF_XDP builders and type aliases for the shared UDP
//! tile runtime.

#![deny(missing_docs)]

use std::fmt;
use std::io;
use std::net::SocketAddrV4;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use fast_socket_rs::{BusyPollDriver, PollDriver, QueueId, UdpRxBuffer, UdpSocket};
pub use fast_socket_udp_tile::{
    AcceptAllClassifier, IngressClassifier, IngressDecision, SocketIndex, SourceAddrClassifier,
    TileConfig, TileError, TileRxBatch, TileRxPacket, TileStats, TileTxBuffer, TileTxMeta,
    TileTxPacket, UdpNetworkTile, UdpNetworkTileHandle, UdpSocketSet, UdpTile, UdpTileHandle,
};
pub use fast_socket_udp_tile::{Park, Spin, TilePollMode};
use fast_socket_xdp_rs::socket::XdpWaitDrivenDriver;
use fast_socket_xdp_rs::{
    InterfaceSelector, RouteSnapshot, XdpFactory, XdpFactoryBuilder, XdpQueueLocalRouter,
    XdpRouteMonitor, XdpRouteMonitorHandle, XdpUdpAggregate, XdpUdpRouter, XdpUdpSocket,
    XdpWorkerPlan,
};

type SpinXdpSocket = XdpUdpSocket<BusyPollDriver, XdpQueueLocalRouter>;
type ParkXdpSocket = XdpUdpSocket<XdpWaitDrivenDriver, XdpQueueLocalRouter>;
type SpinXdpRecvMeta = <SpinXdpSocket as UdpSocket>::RecvMeta;
type SpinXdpRxBuffer = UdpRxBuffer<SpinXdpSocket>;
type ParkXdpRecvMeta = <ParkXdpSocket as UdpSocket>::RecvMeta;
type ParkXdpRxBuffer = UdpRxBuffer<ParkXdpSocket>;
type SpinXdpTile<C> = UdpTile<RouteMonitoredXdpAggregate<BusyPollDriver>, Spin, C>;
type SpinXdpTileHandle<C> = UdpTileHandle<RouteMonitoredXdpAggregate<BusyPollDriver>, Spin, C>;
type ParkXdpTile<C> = UdpTile<RouteMonitoredXdpAggregate<XdpWaitDrivenDriver>, Park, C>;
type ParkXdpTileHandle<C> = UdpTileHandle<RouteMonitoredXdpAggregate<XdpWaitDrivenDriver>, Park, C>;

/// Socket-set wrapper for an existing AF_XDP UDP aggregate.
///
/// Use this with the generic [`UdpTile`] when you already constructed an
/// [`XdpUdpAggregate`] outside the high-level tile builder.
pub struct XdpUdpSocketSet<D, R>
where
    D: PollDriver,
    R: XdpUdpRouter,
{
    aggregate: XdpUdpAggregate<D, R>,
}

impl<D, R> XdpUdpSocketSet<D, R>
where
    D: PollDriver,
    R: XdpUdpRouter,
{
    /// Wraps an AF_XDP UDP aggregate as a tile socket set.
    #[must_use]
    pub const fn new(aggregate: XdpUdpAggregate<D, R>) -> Self {
        Self { aggregate }
    }

    /// Borrows the underlying aggregate.
    #[must_use]
    pub const fn aggregate(&self) -> &XdpUdpAggregate<D, R> {
        &self.aggregate
    }

    /// Mutably borrows the underlying aggregate.
    #[must_use]
    pub const fn aggregate_mut(&mut self) -> &mut XdpUdpAggregate<D, R> {
        &mut self.aggregate
    }

    /// Returns the underlying aggregate.
    #[must_use]
    pub fn into_inner(self) -> XdpUdpAggregate<D, R> {
        self.aggregate
    }
}

impl<D, R> UdpSocketSet for XdpUdpSocketSet<D, R>
where
    D: PollDriver,
    R: XdpUdpRouter,
{
    type Socket = XdpUdpSocket<D, R>;

    fn sockets_mut(&mut self) -> &mut [Self::Socket] {
        self.aggregate.members_mut()
    }

    fn can_transmit_from_any_socket(&self) -> bool {
        self.aggregate.members_share_umem()
    }
}

struct RouteMonitoredXdpAggregate<D>
where
    D: PollDriver,
{
    aggregate: XdpUdpAggregate<D, XdpQueueLocalRouter>,
    route_updates: Vec<XdpRouteMonitorHandle>,
}

impl<D> UdpSocketSet for RouteMonitoredXdpAggregate<D>
where
    D: PollDriver,
{
    type Socket = XdpUdpSocket<D, XdpQueueLocalRouter>;

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
/// wrapper, route-monitor handles, and concrete tile polling mode from application
/// code. It returns an [`XdpUdpTiles`] value that can create lane handles and
/// monitor tile-worker failures.
pub struct XdpUdpTileBuilder<C = SourceAddrClassifier> {
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

impl<C> XdpUdpTileBuilder<C> {
    /// Uses a different ingress classifier.
    #[must_use]
    pub fn classifier<N>(self, classifier: N) -> XdpUdpTileBuilder<N> {
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
    pub fn build(self) -> Result<XdpUdpTiles<C>, TileError>
    where
        C: IngressClassifier<SpinXdpRecvMeta, SpinXdpRxBuffer> + Clone,
    {
        let built = build_tiles(
            self.factory,
            self.local,
            self.lane_count,
            self.config,
            self.classifier,
            self.route_poll_interval,
            |plan, local| plan.open_udp_busy_poll(local),
            "failed to open busy-poll XDP tile aggregate",
        )?;
        Ok(XdpUdpTiles::from_built(built))
    }

    /// Starts one parked tile worker per XDP factory worker plan.
    ///
    /// This opens wait-driven AF_XDP sockets and pairs them with the tile
    /// [`Park`] mode, so the tile can sleep until either RX traffic arrives or
    /// a producer pushes TX work.
    pub fn build_park(self) -> Result<ParkXdpUdpTiles<C>, TileError>
    where
        C: IngressClassifier<ParkXdpRecvMeta, ParkXdpRxBuffer> + Clone,
    {
        let built = build_tiles(
            self.factory,
            self.local,
            self.lane_count,
            self.config,
            self.classifier,
            self.route_poll_interval,
            |plan, local| plan.open_udp_wait_driven(local),
            "failed to open wait-driven XDP tile aggregate",
        )?;
        Ok(ParkXdpUdpTiles::from_built(built))
    }
}

struct BuiltXdpTiles<T> {
    tiles: Vec<Arc<T>>,
    worker_threads: Vec<Option<JoinHandle<Result<(), TileError>>>>,
    taken_lanes: Vec<bool>,
    route_monitor_thread: JoinHandle<()>,
}

fn build_tiles<D, W, C, F>(
    factory: XdpFactory,
    local: SocketAddrV4,
    lane_count: usize,
    config: TileConfig,
    classifier: C,
    route_poll_interval: Duration,
    open: F,
    open_error: &'static str,
) -> Result<BuiltXdpTiles<UdpTile<RouteMonitoredXdpAggregate<D>, W, C>>, TileError>
where
    D: PollDriver + 'static,
    W: TilePollMode,
    D: fast_socket_udp_tile::TilePollModeDriver<W>,
    C: IngressClassifier<
            <XdpUdpSocket<D, XdpQueueLocalRouter> as UdpSocket>::RecvMeta,
            UdpRxBuffer<XdpUdpSocket<D, XdpQueueLocalRouter>>,
        > + Clone,
    F: Fn(XdpWorkerPlan, SocketAddrV4) -> io::Result<XdpUdpAggregate<D, XdpQueueLocalRouter>>
        + Copy
        + Send
        + 'static,
{
    let plans = factory.into_worker_plans();
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
    let route_monitor_thread = route_monitor.start_netlink(monitor_queue, route_poll_interval);

    let mut tiles = Vec::with_capacity(workers.len());
    let mut worker_threads = Vec::with_capacity(workers.len());
    for (tile_index, (plan, route_updates)) in workers.into_iter().enumerate() {
        let tile = Arc::new(UdpTile::with_config(
            move || RouteMonitoredXdpAggregate {
                aggregate: open(plan, local).expect(open_error),
                route_updates,
            },
            classifier.clone(),
            lane_count,
            config,
        ));
        let handle = Arc::clone(&tile).start(tile_index)?;
        tiles.push(tile);
        worker_threads.push(Some(handle));
    }

    Ok(BuiltXdpTiles {
        tiles,
        worker_threads,
        taken_lanes: vec![false; lane_count],
        route_monitor_thread,
    })
}

/// Device-oriented builder for the common busy-poll AF_XDP UDP tile set.
///
/// This builder owns the lower-level [`XdpFactoryBuilder`] until
/// [`Self::build`], so application code can configure worker count and common
/// factory options without manually constructing an [`XdpFactory`].
pub struct XdpUdpTileDeviceBuilder<C = SourceAddrClassifier> {
    factory: XdpFactoryBuilder,
    local: SocketAddrV4,
    lane_count: usize,
    classifier: C,
    config: TileConfig,
    route_poll_interval: Duration,
}

impl<C> XdpUdpTileDeviceBuilder<C> {
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
    pub fn classifier<N>(self, classifier: N) -> XdpUdpTileDeviceBuilder<N> {
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
    pub fn build(self) -> Result<XdpUdpTiles<C>, XdpTileBuildError>
    where
        C: IngressClassifier<SpinXdpRecvMeta, SpinXdpRxBuffer> + Clone,
    {
        let factory = self.factory.build().map_err(XdpTileBuildError::Setup)?;
        XdpUdpTileBuilder::new(factory, self.local, self.lane_count)
            .classifier(self.classifier)
            .config(self.config)
            .route_poll_interval(self.route_poll_interval)
            .build()
            .map_err(XdpTileBuildError::Tile)
    }

    /// Builds the XDP factory and starts one parked tile worker per worker
    /// plan.
    pub fn build_park(self) -> Result<ParkXdpUdpTiles<C>, XdpTileBuildError>
    where
        C: IngressClassifier<ParkXdpRecvMeta, ParkXdpRxBuffer> + Clone,
    {
        let factory = self.factory.build().map_err(XdpTileBuildError::Setup)?;
        XdpUdpTileBuilder::new(factory, self.local, self.lane_count)
            .classifier(self.classifier)
            .config(self.config)
            .route_poll_interval(self.route_poll_interval)
            .build_park()
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
    C: IngressClassifier<SpinXdpRecvMeta, SpinXdpRxBuffer> + Clone,
{
    tiles: Vec<Arc<SpinXdpTile<C>>>,
    worker_threads: Vec<Option<JoinHandle<Result<(), TileError>>>>,
    taken_lanes: Vec<bool>,
    _route_monitor_thread: JoinHandle<()>,
}

impl<C> XdpUdpTiles<C>
where
    C: IngressClassifier<SpinXdpRecvMeta, SpinXdpRxBuffer> + Clone,
{
    fn from_built(built: BuiltXdpTiles<SpinXdpTile<C>>) -> Self {
        Self {
            tiles: built.tiles,
            worker_threads: built.worker_threads,
            taken_lanes: built.taken_lanes,
            _route_monitor_thread: built.route_monitor_thread,
        }
    }

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
    C: IngressClassifier<SpinXdpRecvMeta, SpinXdpRxBuffer> + Clone,
{
    inner: SpinXdpTileHandle<C>,
}

impl<C> UdpNetworkTileHandle for XdpUdpTileHandle<C>
where
    C: IngressClassifier<SpinXdpRecvMeta, SpinXdpRxBuffer> + Clone,
{
    type Socket = SpinXdpSocket;

    fn lane_index(&self) -> usize {
        self.inner.lane_index()
    }

    fn pop_rx_batch(&self) -> Option<TileRxBatch<Self::Socket>> {
        self.inner.pop_rx_batch()
    }

    fn push_tx_buffers(
        &mut self,
        buffers: &mut Vec<TileTxBuffer<Self::Socket>>,
        meta: TileTxMeta,
    ) -> usize {
        self.inner.push_tx_buffers(buffers, meta)
    }

    fn push_tx_packets(&mut self, packets: &mut Vec<TileTxPacket<Self::Socket>>) -> usize {
        self.inner.push_tx_packets(packets)
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
}

/// Started parked wait-driven AF_XDP UDP tiles.
pub struct ParkXdpUdpTiles<C = SourceAddrClassifier>
where
    C: IngressClassifier<ParkXdpRecvMeta, ParkXdpRxBuffer> + Clone,
{
    tiles: Vec<Arc<ParkXdpTile<C>>>,
    worker_threads: Vec<Option<JoinHandle<Result<(), TileError>>>>,
    taken_lanes: Vec<bool>,
    _route_monitor_thread: JoinHandle<()>,
}

impl<C> ParkXdpUdpTiles<C>
where
    C: IngressClassifier<ParkXdpRecvMeta, ParkXdpRxBuffer> + Clone,
{
    fn from_built(built: BuiltXdpTiles<ParkXdpTile<C>>) -> Self {
        Self {
            tiles: built.tiles,
            worker_threads: built.worker_threads,
            taken_lanes: built.taken_lanes,
            _route_monitor_thread: built.route_monitor_thread,
        }
    }

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
    pub fn lane_handles(&mut self, lane_index: usize) -> Option<Vec<ParkXdpUdpTileHandle<C>>> {
        if *self.taken_lanes.get(lane_index)? {
            return None;
        }

        let handles = self
            .tiles
            .iter()
            .map(|tile| {
                Arc::clone(tile)
                    .lane_handle(lane_index)
                    .map(|inner| ParkXdpUdpTileHandle { inner })
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

/// Lane-owned handle for [`ParkXdpUdpTiles`].
pub struct ParkXdpUdpTileHandle<C = SourceAddrClassifier>
where
    C: IngressClassifier<ParkXdpRecvMeta, ParkXdpRxBuffer> + Clone,
{
    inner: ParkXdpTileHandle<C>,
}

impl<C> UdpNetworkTileHandle for ParkXdpUdpTileHandle<C>
where
    C: IngressClassifier<ParkXdpRecvMeta, ParkXdpRxBuffer> + Clone,
{
    type Socket = ParkXdpSocket;

    fn lane_index(&self) -> usize {
        self.inner.lane_index()
    }

    fn pop_rx_batch(&self) -> Option<TileRxBatch<Self::Socket>> {
        self.inner.pop_rx_batch()
    }

    fn push_tx_buffers(
        &mut self,
        buffers: &mut Vec<TileTxBuffer<Self::Socket>>,
        meta: TileTxMeta,
    ) -> usize {
        self.inner.push_tx_buffers(buffers, meta)
    }

    fn push_tx_packets(&mut self, packets: &mut Vec<TileTxPacket<Self::Socket>>) -> usize {
        self.inner.push_tx_packets(packets)
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
