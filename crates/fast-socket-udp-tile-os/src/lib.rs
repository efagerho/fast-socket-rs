//! Operating-system UDP network-tile orchestration.
//!
//! This crate provides OS-socket builders and type aliases for the shared UDP
//! tile runtime.

#![deny(missing_docs)]

use std::net::SocketAddr;
use std::sync::Arc;

use fast_socket_os_rs::{OsUdpSocket, OsUdpSocketBuilder};
use fast_socket_rs::{QueueId, UdpRxBuffer, UdpSocket};
pub use fast_socket_udp_tile::{
    AcceptAllClassifier, IngressClassifier, IngressDecision, SocketIndex, SourceAddrClassifier,
    TileConfig, TileError, TileRxBatch, TileRxPacket, TileStats, TileTxBuffer, TileTxMeta,
    TileTxPacket, UdpNetworkTile, UdpNetworkTileHandle, UdpSocketSet, UdpTile, UdpTileHandle,
};
pub use fast_socket_udp_tile::{Park, TilePollMode};

type DefaultOsRecvMeta = <OsUdpSocket as UdpSocket>::RecvMeta;
type DefaultOsRxBuffer = UdpRxBuffer<OsUdpSocket>;
type OsSocketConfigurator =
    Box<dyn Fn(usize, OsUdpSocketBuilder) -> OsUdpSocketBuilder + Send + Sync + 'static>;

/// OS-backed UDP tile over a vector of [`OsUdpSocket`] members.
pub type OsUdpTile<C> = UdpTile<Vec<OsUdpSocket>, Park, C>;

/// Builder for the common OS UDP tile shape.
///
/// The builder hides the concrete socket-set type from application code and
/// creates a wait-driven parked tile.
pub struct OsUdpTileBuilder<C = SourceAddrClassifier>
where
    C: IngressClassifier<DefaultOsRecvMeta, DefaultOsRxBuffer>,
{
    factory: Box<dyn FnOnce() -> Vec<OsUdpSocket> + Send + 'static>,
    classifier: C,
    lane_count: usize,
    config: TileConfig,
}

impl OsUdpTileBuilder<SourceAddrClassifier> {
    /// Creates a builder using [`SourceAddrClassifier`] and default tile config.
    #[must_use]
    pub fn new(
        factory: impl FnOnce() -> Vec<OsUdpSocket> + Send + 'static,
        lane_count: usize,
    ) -> Self {
        Self {
            factory: Box::new(factory),
            classifier: SourceAddrClassifier,
            lane_count,
            config: TileConfig::default(),
        }
    }

    /// Creates a builder that opens `socket_count` `SO_REUSEPORT` sockets.
    ///
    /// Sockets are opened inside the tile worker through
    /// [`OsUdpSocketBuilder`], so applications do not need to hand-write the
    /// repeated socket-set factory. Use
    /// [`OsUdpTileReusePortBuilder::configure_socket`] for per-socket queue,
    /// affinity, MTU, and buffer-layout customization.
    #[must_use]
    pub fn reuse_port(
        bind_addr: SocketAddr,
        socket_count: usize,
        lane_count: usize,
    ) -> OsUdpTileReusePortBuilder<SourceAddrClassifier> {
        OsUdpTileReusePortBuilder {
            bind_addr,
            bind_device: None,
            socket_count,
            lane_count,
            classifier: SourceAddrClassifier,
            config: TileConfig::default(),
            configure_socket: Box::new(|_, builder| builder),
        }
    }
}

impl<C> OsUdpTileBuilder<C>
where
    C: IngressClassifier<DefaultOsRecvMeta, DefaultOsRxBuffer>,
{
    /// Uses a different ingress classifier.
    #[must_use]
    pub fn classifier<N>(self, classifier: N) -> OsUdpTileBuilder<N>
    where
        N: IngressClassifier<DefaultOsRecvMeta, DefaultOsRxBuffer>,
    {
        OsUdpTileBuilder {
            factory: self.factory,
            classifier,
            lane_count: self.lane_count,
            config: self.config,
        }
    }

    /// Uses an explicit tile configuration.
    #[must_use]
    pub fn config(mut self, config: TileConfig) -> Self {
        self.config = config;
        self
    }

    /// Builds a wait-driven parked tile.
    #[must_use]
    pub fn build(self) -> Arc<OsUdpTile<C>> {
        Arc::new(UdpTile::with_config(
            self.factory,
            self.classifier,
            self.lane_count,
            self.config,
        ))
    }
}

/// Builder for an OS UDP tile backed by repeated `SO_REUSEPORT` sockets.
pub struct OsUdpTileReusePortBuilder<C = SourceAddrClassifier>
where
    C: IngressClassifier<DefaultOsRecvMeta, DefaultOsRxBuffer>,
{
    bind_addr: SocketAddr,
    bind_device: Option<String>,
    socket_count: usize,
    lane_count: usize,
    classifier: C,
    config: TileConfig,
    configure_socket: OsSocketConfigurator,
}

impl<C> OsUdpTileReusePortBuilder<C>
where
    C: IngressClassifier<DefaultOsRecvMeta, DefaultOsRxBuffer>,
{
    /// Uses a different ingress classifier.
    #[must_use]
    pub fn classifier<N>(self, classifier: N) -> OsUdpTileReusePortBuilder<N>
    where
        N: IngressClassifier<DefaultOsRecvMeta, DefaultOsRxBuffer>,
    {
        OsUdpTileReusePortBuilder {
            bind_addr: self.bind_addr,
            bind_device: self.bind_device,
            socket_count: self.socket_count,
            lane_count: self.lane_count,
            classifier,
            config: self.config,
            configure_socket: self.configure_socket,
        }
    }

    /// Uses an explicit tile configuration.
    #[must_use]
    pub fn config(mut self, config: TileConfig) -> Self {
        self.config = config;
        self
    }

    /// Binds every socket in the set to an operating-system network device.
    ///
    /// This uses [`OsUdpSocketBuilder::bind_to_device`]. On Linux that maps to
    /// `SO_BINDTODEVICE`; on other platforms the underlying socket open fails
    /// with an unsupported-operation error if the tile worker starts.
    #[must_use]
    pub fn bind_device(mut self, device: impl Into<String>) -> Self {
        self.bind_device = Some(device.into());
        self
    }

    /// Customizes each [`OsUdpSocketBuilder`] before it is bound.
    ///
    /// The callback receives the zero-based socket index. The default socket
    /// builder has `SO_REUSEPORT` enabled and a [`QueueId`] matching that
    /// index.
    #[must_use]
    pub fn configure_socket(
        mut self,
        configure: impl Fn(usize, OsUdpSocketBuilder) -> OsUdpSocketBuilder + Send + Sync + 'static,
    ) -> Self {
        self.configure_socket = Box::new(configure);
        self
    }

    /// Builds a wait-driven parked tile.
    #[must_use]
    pub fn build(self) -> Arc<OsUdpTile<C>> {
        let Self {
            bind_addr,
            bind_device,
            socket_count,
            lane_count,
            classifier,
            config,
            configure_socket,
        } = self;
        Arc::new(UdpTile::with_config(
            reuse_port_socket_factory(bind_addr, bind_device, socket_count, configure_socket),
            classifier,
            lane_count,
            config,
        ))
    }
}

fn reuse_port_socket_factory(
    bind_addr: SocketAddr,
    bind_device: Option<String>,
    socket_count: usize,
    configure_socket: OsSocketConfigurator,
) -> impl FnOnce() -> Vec<OsUdpSocket> + Send + 'static {
    move || {
        (0..socket_count)
            .map(|index| {
                let queue_id = u32::try_from(index)
                    .map(QueueId::new)
                    .expect("OS UDP tile socket index does not fit QueueId");
                let mut builder = OsUdpSocketBuilder::new(bind_addr)
                    .reuse_port(true)
                    .queue_id(queue_id);
                if let Some(device) = &bind_device {
                    builder = builder.bind_to_device(device.clone());
                }
                configure_socket(index, builder)
                    .bind()
                    .expect("failed to open OS UDP tile socket")
            })
            .collect()
    }
}
