//! Builders and configuration for AF_XDP IP packet sockets.

use core::num::NonZeroUsize;
use std::io;
use std::net::SocketAddrV4;

use fast_socket_rs::{BufferLayout, HugePageSize, IfIndex, NumaNode, QueueBufferConfig, QueueId};

use crate::program::{AttachMode, XdpProgramHandle};
use crate::raw_socket::{RingSizes, XdpMode};
use crate::route::RouteSnapshot;
use crate::socket::{
    BusyPollXdpIpPacketSocket, BusyPollXdpUdpSocket, ReadinessXdpIpPacketSocket,
    ReadinessXdpUdpSocket, XdpIpPacketSocket, XdpQueueLocalRouter, XdpUdpRouter, XdpUdpSocket,
};

/// Configuration for one AF_XDP queue socket.
#[derive(Clone, Debug)]
pub struct XdpIpPacketSocketConfig {
    /// Interface index.
    pub ifindex: IfIndex,
    /// Queue id.
    pub queue_id: QueueId,
    /// Optional NUMA node.
    pub numa_node: Option<NumaNode>,
    /// Hugepage preference for UMEM.
    pub huge_page_size: HugePageSize,
    /// Per-queue buffer configuration.
    pub buffers: QueueBufferConfig,
    /// AF_XDP ring sizes.
    pub rings: RingSizes,
    /// Requested AF_XDP mode.
    pub mode: XdpMode,
    /// IP-layer MTU.
    pub mtu: usize,
    /// Total number of UMEM frames for live AF_XDP sockets.
    pub frame_count: u32,
    /// XDP program attach mode for live sockets.
    pub attach_mode: AttachMode,
    /// Optional custom eBPF object bytes for live sockets.
    pub program_bytes: Option<&'static [u8]>,
    /// Optional pre-attached XDP program to reuse for live sockets.
    pub attached_program: Option<XdpProgramHandle>,
    /// Optional UDP destination port for UDP-filtered redirect programs.
    pub bind_udp_port: Option<u16>,
    /// Initial queue-local route, neighbor, and link snapshot.
    pub route_snapshot: RouteSnapshot,
}

impl Default for XdpIpPacketSocketConfig {
    fn default() -> Self {
        let align = NonZeroUsize::new(2048).expect("non-zero alignment");
        let rx = BufferLayout::with_headroom_and_tailroom(2048, 0, 0)
            .with_l2_headroom(64)
            .with_alignment(align)
            .with_fixed_chunk(4096, 4096)
            .expect("default XDP rx layout is valid");
        let tx = BufferLayout::with_headroom_and_tailroom(2048, 64, 0)
            .with_l2_headroom(64)
            .with_alignment(align)
            .with_fixed_chunk(4096, 4096)
            .expect("default XDP tx layout is valid");
        Self {
            ifindex: IfIndex::new(0),
            queue_id: QueueId::new(0),
            numa_node: None,
            huge_page_size: HugePageSize::Default,
            buffers: QueueBufferConfig {
                rx,
                tx,
                rx_depth: Some(2048),
                tx_depth: Some(2048),
            },
            rings: RingSizes::default(),
            mode: XdpMode::ZeroCopy,
            mtu: 1500,
            frame_count: 4096,
            attach_mode: AttachMode::Default,
            program_bytes: None,
            attached_program: None,
            bind_udp_port: None,
            route_snapshot: RouteSnapshot::new(),
        }
    }
}

/// Builder for an AF_XDP IP packet socket.
#[derive(Clone, Debug)]
pub struct XdpIpPacketSocketBuilder {
    config: XdpIpPacketSocketConfig,
}

/// Builder for an AF_XDP UDP socket.
#[derive(Clone, Debug)]
pub struct XdpUdpSocketBuilder<R = XdpQueueLocalRouter> {
    config: XdpIpPacketSocketConfig,
    local_addr: SocketAddrV4,
    router: R,
}

impl XdpIpPacketSocketBuilder {
    /// Creates a builder for `ifindex` and `queue_id`.
    #[must_use]
    pub fn new(ifindex: IfIndex, queue_id: QueueId) -> Self {
        Self {
            config: XdpIpPacketSocketConfig {
                ifindex,
                queue_id,
                ..XdpIpPacketSocketConfig::default()
            },
        }
    }

    /// Sets queue buffer configuration.
    #[must_use]
    pub fn buffers(mut self, buffers: QueueBufferConfig) -> Self {
        self.config.buffers = buffers;
        self
    }

    /// Sets the NUMA node hint.
    #[must_use]
    pub const fn numa_node(mut self, numa_node: NumaNode) -> Self {
        self.config.numa_node = Some(numa_node);
        self
    }

    /// Sets the hugepage preference.
    #[must_use]
    pub const fn huge_page_size(mut self, huge_page_size: HugePageSize) -> Self {
        self.config.huge_page_size = huge_page_size;
        self
    }

    /// Sets ring sizes.
    #[must_use]
    pub const fn rings(mut self, rings: RingSizes) -> Self {
        self.config.rings = rings;
        self
    }

    /// Sets AF_XDP mode.
    #[must_use]
    pub const fn mode(mut self, mode: XdpMode) -> Self {
        self.config.mode = mode;
        self
    }

    /// Sets the IP-layer MTU.
    #[must_use]
    pub const fn mtu(mut self, mtu: usize) -> Self {
        self.config.mtu = mtu;
        self
    }

    /// Sets the total UMEM frame count for live sockets.
    #[must_use]
    pub const fn frame_count(mut self, frame_count: u32) -> Self {
        self.config.frame_count = frame_count;
        self
    }

    /// Sets the XDP attach mode for live sockets.
    #[must_use]
    pub const fn attach_mode(mut self, attach_mode: AttachMode) -> Self {
        self.config.attach_mode = attach_mode;
        self
    }

    /// Sets custom XDP program bytes for live sockets.
    #[must_use]
    pub const fn program_bytes(mut self, program_bytes: &'static [u8]) -> Self {
        self.config.program_bytes = Some(program_bytes);
        self
    }

    /// Reuses a pre-attached XDP program for live sockets.
    #[must_use]
    pub fn attached_program(mut self, program: XdpProgramHandle) -> Self {
        self.config.attached_program = Some(program);
        self
    }

    /// Enables one UDP destination port when the loaded XDP program has a
    /// `BOUND_PORTS` map. The bundled program uses this to pass unrelated IP
    /// traffic back to the kernel while UDP sockets are bound.
    #[must_use]
    pub const fn bind_udp_port(mut self, port: u16) -> Self {
        self.config.bind_udp_port = Some(port);
        self
    }

    /// Seeds the queue-local route, neighbor, and link cache.
    #[must_use]
    pub fn route_snapshot(mut self, snapshot: RouteSnapshot) -> Self {
        self.config.route_snapshot = snapshot;
        self
    }

    /// Builds the first-pass busy-poll IP packet socket.
    #[must_use]
    pub fn open_busy_poll(self) -> BusyPollXdpIpPacketSocket {
        XdpIpPacketSocket::new_busy_poll(self.config)
    }

    /// Builds a live busy-poll AF_XDP IP packet socket.
    pub fn open_busy_poll_live(self) -> io::Result<BusyPollXdpIpPacketSocket> {
        XdpIpPacketSocket::new_busy_poll_live(self.config)
    }

    /// Builds a live readiness-driven AF_XDP IP packet socket.
    pub fn open_readiness_live(self) -> io::Result<ReadinessXdpIpPacketSocket> {
        XdpIpPacketSocket::new_readiness_live(self.config)
    }

    /// Returns the accumulated configuration.
    #[must_use]
    pub fn into_config(self) -> XdpIpPacketSocketConfig {
        self.config
    }
}

impl XdpUdpSocketBuilder<XdpQueueLocalRouter> {
    /// Creates a builder for `ifindex`, `queue_id`, and a local IPv4 UDP address.
    #[must_use]
    pub fn new(ifindex: IfIndex, queue_id: QueueId, local_addr: SocketAddrV4) -> Self {
        Self {
            config: XdpIpPacketSocketConfig {
                ifindex,
                queue_id,
                bind_udp_port: Some(local_addr.port()),
                ..XdpIpPacketSocketConfig::default()
            },
            local_addr,
            router: XdpQueueLocalRouter::default(),
        }
    }

    /// Seeds the queue-local route, neighbor, and link cache.
    #[must_use]
    pub fn route_snapshot(mut self, snapshot: RouteSnapshot) -> Self {
        self.config.route_snapshot = snapshot.clone();
        self.router = XdpQueueLocalRouter::new(snapshot);
        self
    }
}

impl<R> XdpUdpSocketBuilder<R> {
    /// Sets queue buffer configuration.
    #[must_use]
    pub fn buffers(mut self, buffers: QueueBufferConfig) -> Self {
        self.config.buffers = buffers;
        self
    }

    /// Sets the NUMA node hint.
    #[must_use]
    pub const fn numa_node(mut self, numa_node: NumaNode) -> Self {
        self.config.numa_node = Some(numa_node);
        self
    }

    /// Sets the hugepage preference.
    #[must_use]
    pub const fn huge_page_size(mut self, huge_page_size: HugePageSize) -> Self {
        self.config.huge_page_size = huge_page_size;
        self
    }

    /// Sets ring sizes.
    #[must_use]
    pub const fn rings(mut self, rings: RingSizes) -> Self {
        self.config.rings = rings;
        self
    }

    /// Sets AF_XDP mode.
    #[must_use]
    pub const fn mode(mut self, mode: XdpMode) -> Self {
        self.config.mode = mode;
        self
    }

    /// Sets the IP-layer MTU used to size UDP datagrams.
    #[must_use]
    pub const fn mtu(mut self, mtu: usize) -> Self {
        self.config.mtu = mtu;
        self
    }

    /// Sets the total UMEM frame count for live sockets.
    #[must_use]
    pub const fn frame_count(mut self, frame_count: u32) -> Self {
        self.config.frame_count = frame_count;
        self
    }

    /// Sets the XDP attach mode for live sockets.
    #[must_use]
    pub const fn attach_mode(mut self, attach_mode: AttachMode) -> Self {
        self.config.attach_mode = attach_mode;
        self
    }

    /// Sets custom XDP program bytes for live sockets.
    #[must_use]
    pub const fn program_bytes(mut self, program_bytes: &'static [u8]) -> Self {
        self.config.program_bytes = Some(program_bytes);
        self
    }

    /// Reuses a pre-attached XDP program for live sockets.
    #[must_use]
    pub fn attached_program(mut self, program: XdpProgramHandle) -> Self {
        self.config.attached_program = Some(program);
        self
    }

    /// Sets the UDP destination port used by UDP-filtered redirect programs.
    #[must_use]
    pub const fn bind_udp_port(mut self, port: u16) -> Self {
        self.config.bind_udp_port = Some(port);
        self
    }

    /// Disables UDP destination-port filtering for the loaded XDP program.
    #[must_use]
    pub const fn without_udp_port_filter(mut self) -> Self {
        self.config.bind_udp_port = None;
        self
    }

    /// Uses a custom UDP egress router.
    #[must_use]
    pub fn router<N>(self, router: N) -> XdpUdpSocketBuilder<N> {
        XdpUdpSocketBuilder {
            config: self.config,
            local_addr: self.local_addr,
            router,
        }
    }

    /// Builds the first-pass busy-poll UDP socket.
    #[must_use]
    pub fn open_busy_poll(self) -> BusyPollXdpUdpSocket<R>
    where
        R: XdpUdpRouter,
    {
        let ip = XdpIpPacketSocket::new_busy_poll(self.config);
        XdpUdpSocket::from_ip_socket(ip, self.local_addr, self.router)
    }

    /// Builds a live busy-poll AF_XDP UDP socket.
    pub fn open_busy_poll_live(self) -> io::Result<BusyPollXdpUdpSocket<R>>
    where
        R: XdpUdpRouter,
    {
        let ip = XdpIpPacketSocket::new_busy_poll_live(self.config)?;
        Ok(XdpUdpSocket::from_ip_socket(
            ip,
            self.local_addr,
            self.router,
        ))
    }

    /// Builds a live readiness-driven AF_XDP UDP socket.
    pub fn open_readiness_live(self) -> io::Result<ReadinessXdpUdpSocket<R>>
    where
        R: XdpUdpRouter,
    {
        let ip = XdpIpPacketSocket::new_readiness_live(self.config)?;
        Ok(XdpUdpSocket::from_ip_socket(
            ip,
            self.local_addr,
            self.router,
        ))
    }
}
