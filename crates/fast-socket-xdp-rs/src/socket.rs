//! AF_XDP `IpPacketSocket` implementation.

use std::collections::VecDeque;
use std::fmt;
use std::marker::PhantomData;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::rc::Rc;
use std::time::Duration;

#[cfg(test)]
use fast_socket_rs::OwnedPacketBuffer;
use fast_socket_rs::{
    BufferPool, BusyPollDriver, Capabilities, ChecksumStatus, DeviceError, DeviceErrorKind,
    EgressResolver, Error, IfIndex, IpPacketReceive, IpPacketRecvMeta, IpPacketSocket,
    IpPacketTransmit, IpVersion, NumaNode, PacketBuffer, PacketBufferMut, PollDriver,
    QueueAffinity, QueueId, RawDevice, RawDeviceStats, ReadinessDriver, ReadinessSource, RecvBatch,
    SendError, SocketId, TxSlot, UdpCapabilities, UdpReceive, UdpRecvMeta, UdpSocket, UdpTransmit,
    V4Only, WaitOutcome, WakeHandle,
};

use crate::buffer::{FrameReclaim, XdpPacketBuf, XdpPacketBufMut, XdpRxPool, XdpTxPool};
use crate::config::XdpIpPacketSocketConfig;
use crate::egress::{
    ETHERNET_HEADER_LEN, ETHERTYPE_IPV4, ETHERTYPE_IPV6, ResolvedL2, VLAN_ETHERTYPE,
    VLAN_HEADER_LEN, XdpEgress, XdpResolvedEgress, ethernet_header_len, write_ethernet_header,
};
use crate::interface::{if_index_to_name, numa_node_for_interface};
use crate::program::XdpProgramHandle;
use crate::raw_socket::RawXdpSocket;
use crate::ring::XdpDesc;
use crate::route::{RouteSnapshot, XdpLocalRoutes};
use crate::umem::Umem;

const IPV4_MIN_HEADER_LEN: usize = 20;
const IPV6_HEADER_LEN: usize = 40;
const IPV4_FRAGMENT_MASK: u16 = 0x3fff;
const IPV6_NEXT_HEADER_FRAGMENT: u8 = 44;
const UDP_HEADER_LEN: usize = 8;
const UDP_PROTOCOL: u8 = 17;

/// Default receive metadata for AF_XDP IP packet sockets.
pub type XdpIpPacketRecvMeta = IpPacketRecvMeta;

/// Busy-poll AF_XDP IP packet socket.
pub type BusyPollXdpIpPacketSocket = XdpIpPacketSocket<BusyPollDriver>;

/// Readiness-driven AF_XDP IP packet socket.
pub type ReadinessXdpIpPacketSocket = XdpIpPacketSocket<ReadinessDriver<XdpReadinessSource>>;

/// Busy-poll AF_XDP UDP socket.
pub type BusyPollXdpUdpSocket<R = XdpQueueLocalRouter> = XdpUdpSocket<BusyPollDriver, R>;

/// Readiness-driven AF_XDP UDP socket.
pub type ReadinessXdpUdpSocket<R = XdpQueueLocalRouter> =
    XdpUdpSocket<ReadinessDriver<XdpReadinessSource>, R>;

/// Readiness source backed by a borrowed AF_XDP fd clone.
#[derive(Debug)]
pub struct XdpReadinessSource {
    fd: OwnedFd,
}

impl XdpReadinessSource {
    /// Creates a readiness source from an owned fd.
    #[must_use]
    pub const fn new(fd: OwnedFd) -> Self {
        Self { fd }
    }
}

impl ReadinessSource for XdpReadinessSource {
    fn wait(&mut self, timeout: Option<Duration>) -> Result<WaitOutcome, Error> {
        wait_for_readable(self.fd.as_fd(), timeout)
    }

    fn wake_handle(&self) -> Option<WakeHandle<'_>> {
        Some(WakeHandle::from_fd(self.fd.as_fd()))
    }
}

/// AF_XDP IP packet socket state.
#[derive(Debug)]
pub struct XdpIpPacketSocket<D = BusyPollDriver> {
    // DROP ORDER IS LOAD-BEARING: these two buffer queues hold `XdpPacketBuf`s
    // that borrow `rx_pool`/`tx_pool`/`live` (UMEM + reclaim) through raw
    // pointers and reclaim into them on drop. Struct fields drop in declaration
    // order, so the queues MUST be declared before the pools and `live`; dropping
    // a pool first would free the reclaim state out from under a buffer still
    // queued here (a use-after-free, caught in debug by the buffer owner-epoch
    // guard). Do not reorder these below the pools.
    pending_rx: VecDeque<IpPacketReceive<XdpPacketBufMut, XdpIpPacketRecvMeta>>,
    pending_tx_frames: VecDeque<XdpPacketBuf>,
    config: XdpIpPacketSocketConfig,
    /// Single backing NIC queue, stored so [`RawDevice::nic_queues`] can return
    /// a slice. Always one element for this single-queue socket; aggregate
    /// sockets carry several.
    nic_queues: [QueueId; 1],
    rx_pool: XdpRxPool,
    tx_pool: XdpTxPool,
    driver: D,
    routes: XdpLocalRoutes,
    live: Option<LiveXdpState>,
    stats: RawDeviceStats,
    _not_send: PhantomData<Rc<()>>,
}

/// AF_XDP UDP socket state.
///
/// The transmit path builds UDP/IPv4 and Ethernet headers before enqueueing
/// AF_XDP descriptors, and the receive path parses Ethernet, IPv4, and UDP in
/// one backend pass before wrapping the UDP payload.
#[derive(Debug)]
pub struct XdpUdpSocket<D = BusyPollDriver, R = XdpQueueLocalRouter> {
    ip: XdpIpPacketSocket<D>,
    local_addr: SocketAddrV4,
    router: R,
    ttl: u8,
}

/// Resolves UDP destinations into AF_XDP transmit egress handles.
///
/// Implementors may own route and neighbor state, perform dynamic lookups, or
/// delegate to another egress lookup implementation.
pub trait XdpUdpRouter {
    /// Resolves one IPv4 UDP destination for an AF_XDP queue.
    fn resolve_udp_egress(&self, dst: Ipv4Addr, context: XdpRouteContext) -> Option<XdpEgress>;

    /// Resolves one IPv4 UDP destination and may return prebuilt L2 bytes.
    ///
    /// The default implementation materializes the header from
    /// [`Self::resolve_udp_egress`]. Routers that cache destinations should
    /// override this method to avoid rebuilding L2 bytes on every send.
    fn resolve_udp_egress_resolved(
        &self,
        dst: Ipv4Addr,
        context: XdpRouteContext,
    ) -> Option<XdpResolvedEgress> {
        self.resolve_udp_egress(dst, context)
            .map(XdpResolvedEgress::from_egress)
    }

    /// Resolves a destination to just the L2 header and effective MTU on the
    /// transmit hot path, borrowing a prebuilt header when the router can.
    ///
    /// The default builds an inline header from
    /// [`Self::resolve_udp_egress_resolved`] and rejects any egress that does
    /// not match `context` (wrong interface/queue) or is not IPv4. Routers
    /// backed by a queue-local snapshot override this to return the prebuilt
    /// header **by reference** with no per-packet copy, queue stamp, or
    /// revalidation.
    fn resolve_udp_l2(&self, dst: Ipv4Addr, context: XdpRouteContext) -> Option<ResolvedL2<'_>> {
        let resolved = self.resolve_udp_egress_resolved(dst, context)?;
        let egress = resolved.egress();
        if egress.ifindex != context.ifindex
            || egress.queue != context.queue
            || egress.ethertype != ETHERTYPE_IPV4
        {
            return None;
        }
        Some(ResolvedL2::Inline {
            l2_header: resolved.l2_header_array(),
            l2_len: resolved.l2_len(),
            ip_mtu: context.mtu.min(resolved.mtu() as usize),
        })
    }
}

/// Per-queue facts available to UDP egress routers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct XdpRouteContext {
    /// AF_XDP interface index owned by the socket.
    pub ifindex: IfIndex,
    /// AF_XDP queue id owned by the socket.
    pub queue: QueueId,
    /// Socket IP-layer MTU.
    pub mtu: usize,
}

/// UDP router backed by queue-local route and neighbor snapshots.
#[derive(Clone, Debug)]
pub struct XdpQueueLocalRouter {
    routes: XdpLocalRoutes,
}

impl XdpQueueLocalRouter {
    /// Creates a queue-local UDP router from an initial route snapshot.
    #[must_use]
    pub fn new(snapshot: RouteSnapshot) -> Self {
        Self {
            routes: XdpLocalRoutes::new(snapshot),
        }
    }

    /// Returns queue-local route state.
    #[must_use]
    pub fn routes(&self) -> &XdpLocalRoutes {
        &self.routes
    }

    /// Returns queue-local route state mutably for cold-path update adoption.
    #[must_use]
    pub fn routes_mut(&mut self) -> &mut XdpLocalRoutes {
        &mut self.routes
    }

    /// Applies queued route snapshot updates outside the packet path.
    pub fn apply_route_updates(&mut self) -> usize {
        self.routes.apply_updates()
    }
}

impl Default for XdpQueueLocalRouter {
    fn default() -> Self {
        Self::new(RouteSnapshot::new())
    }
}

impl XdpUdpRouter for XdpQueueLocalRouter {
    fn resolve_udp_egress(&self, dst: Ipv4Addr, context: XdpRouteContext) -> Option<XdpEgress> {
        self.routes
            .resolve_v4_for_interface(dst, context.ifindex, context.queue)
    }

    fn resolve_udp_egress_resolved(
        &self,
        dst: Ipv4Addr,
        context: XdpRouteContext,
    ) -> Option<XdpResolvedEgress> {
        self.routes
            .resolve_v4_resolved_for_interface(dst, context.ifindex, context.queue)
    }

    #[inline]
    fn resolve_udp_l2(&self, dst: Ipv4Addr, context: XdpRouteContext) -> Option<ResolvedL2<'_>> {
        // The egress stored for `context.ifindex` is already correct for this
        // socket, so we skip the queue stamp + revalidation and borrow the
        // prebuilt header directly.
        self.routes
            .snapshot()
            .resolve_l2_for_interface(dst, context.ifindex, context.mtu)
    }
}

impl<T> XdpUdpRouter for T
where
    T: EgressResolver<V4Only, XdpEgress>,
{
    fn resolve_udp_egress(&self, dst: Ipv4Addr, _context: XdpRouteContext) -> Option<XdpEgress> {
        EgressResolver::resolve_egress(self, dst)
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct XdpUdpTxContext {
    destination: SocketAddr,
    source_ip: Option<IpAddr>,
    ecn: Option<fast_socket_rs::EcnCodepoint>,
    gso_segment_size: Option<core::num::NonZeroU16>,
}

// Test-only scaffolding for the heap-backed `send_udp_test` path. The live
// transmit path (`send_udp_inner`) resolves egress via `XdpUdpRouter::resolve_udp_l2`,
// which borrows the prebuilt header instead of copying a full egress.
#[cfg(test)]
#[derive(Clone, Copy, Debug)]
struct UdpEgressContext {
    ifindex: IfIndex,
    queue_id: QueueId,
    mtu: usize,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
struct ResolvedUdpEgress {
    l2_header: [u8; VLAN_HEADER_LEN],
    l2_len: usize,
    ip_mtu: usize,
}

struct LiveXdpState {
    raw: RawXdpSocket,
    umem: Rc<Umem>,
    rx_reclaim: Rc<FrameReclaim>,
    /// First UMEM frame owned by the TX pool; lower frame addresses return to FILL.
    first_tx_frame_addr: u64,
    program: Option<XdpProgramHandle>,
    bound_port: Option<u16>,
    numa_node: NumaNode,
    pending_fill_scratch: Vec<u64>,
    rx_descs: Vec<XdpDesc>,
    tx_descs: Vec<XdpDesc>,
    tx_in_flight: usize,
    tx_since_completion_drain: usize,
    /// Cached `min(rings.completion/2, frame_count/4)` from socket construction.
    /// The pressure check fires whenever `tx_in_flight` reaches this many
    /// outstanding TX frames; precomputing it avoids three integer divisions
    /// and two `max(1)`s per send.
    tx_completion_drain_threshold: usize,
    /// Cached `max(1, tx_completion_drain_threshold / 2)` from socket
    /// construction. Together with the threshold it forms a hysteresis pair:
    /// the threshold turns draining on, the interval limits how aggressively
    /// the drain repeats once active.
    tx_completion_drain_interval: usize,
    /// Set when a `wake_tx` doorbell failed after a successful commit; the
    /// next send (or completion drain) reissues the doorbell so the parked TX
    /// descriptors get a chance to land on the wire.
    tx_wake_pending: bool,
}

struct OpenedLiveXdp {
    state: LiveXdpState,
    rx_pool: XdpRxPool,
    tx_pool: XdpTxPool,
}

impl fmt::Debug for LiveXdpState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiveXdpState")
            .field("raw", &self.raw)
            .field("bound_port", &self.bound_port)
            .finish_non_exhaustive()
    }
}

impl Drop for LiveXdpState {
    fn drop(&mut self) {
        let queue_id = self.raw.queue_id();
        let Some(program) = self.program.take() else {
            return;
        };
        let mut guard = match program.lock() {
            Ok(guard) => guard,
            Err(_) => {
                eprintln!(
                    "fast-socket-xdp: XDP program mutex poisoned during LiveXdpState drop for queue {queue_id}; \
                     skipping unbind/unregister"
                );
                return;
            }
        };
        if let Some(port) = self.bound_port
            && let Err(error) = guard.unbind_port(port)
        {
            eprintln!(
                "fast-socket-xdp: failed to unbind UDP port {port} for queue {queue_id} during drop: {error}"
            );
        }
        if let Err(error) = guard.unregister_socket(queue_id) {
            eprintln!(
                "fast-socket-xdp: failed to unregister AF_XDP queue {queue_id} from XSKMAP during drop: {error}"
            );
        }
    }
}

impl LiveXdpState {
    fn open(config: &XdpIpPacketSocketConfig) -> std::io::Result<OpenedLiveXdp> {
        let numa_node = resolve_umem_numa_node(config)?;
        let program = match &config.attached_program {
            Some(program) => {
                if program.if_index() != config.ifindex.get() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "attached XDP program if_index {} does not match socket if_index {}",
                            program.if_index(),
                            config.ifindex.get()
                        ),
                    ));
                }
                program.clone()
            }
            None => XdpProgramHandle::load(
                config.ifindex.get(),
                config.attach_mode,
                config.program_bytes,
            )?,
        };

        let frame_size = umem_frame_size(config)?;
        // AF_XDP requires a power-of-two `frame_count`. Reject mis-sized
        // configs explicitly so callers see the surprise at construction
        // rather than learning later that their "3000-frame UMEM" is
        // actually 4096 frames and their capacity math is off.
        let frame_count = config.frame_count;
        if frame_count < 2 || !frame_count.is_power_of_two() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("XDP frame_count must be a power of two >= 2 (got {frame_count})"),
            ));
        }
        let rx_frames = frame_count / 2;
        let mut umem =
            Umem::new_on_numa_node(frame_size, frame_count, config.huge_page_size, numa_node)?;

        let pre_fill = (0..rx_frames)
            .map(|index| umem.frame_offset(index))
            .collect::<Vec<_>>();
        let umem_headroom = config
            .buffers
            .rx
            .l2_headroom()
            .try_into()
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
        let raw = RawXdpSocket::new_with_umem_headroom(
            config.ifindex.get(),
            config.queue_id.get(),
            &mut umem,
            config.rings,
            config.mode,
            umem_headroom,
            pre_fill,
        )?;
        let umem = Rc::new(umem);

        let mut bound_port = None;
        {
            let mut guard = program.lock().expect("XDP program mutex poisoned");
            guard.register_socket(config.queue_id.get(), raw.as_fd())?;
            if let Some(port) = config.bind_udp_port {
                if let Err(error) = guard.bind_port(port) {
                    if let Err(rollback) = guard.unregister_socket(config.queue_id.get()) {
                        eprintln!(
                            "fast-socket-xdp: failed to roll back AF_XDP queue {} after \
                             bind_port({port}) failed: {rollback}",
                            config.queue_id.get(),
                        );
                    }
                    return Err(error);
                }
                bound_port = Some(port);
            }
        }

        let first_tx_frame_addr = umem.frame_offset(rx_frames);
        let tx_frames = (rx_frames..frame_count)
            .map(|index| umem.frame_offset(index))
            .collect::<Vec<_>>();
        let rx_reclaim = FrameReclaim::new(Vec::with_capacity(rx_frames as usize));
        let tx_reclaim = FrameReclaim::new(tx_frames);
        let rx_pool = XdpRxPool::live(config.buffers.rx, Rc::clone(&umem), Rc::clone(&rx_reclaim));
        let tx_pool = XdpTxPool::live(config.buffers.tx, Rc::clone(&umem), Rc::clone(&tx_reclaim));

        // Precompute the TX completion-drain hysteresis pair. Both values
        // are immutable for the lifetime of the socket; recomputing them
        // on every send adds ~7% to the per-call cost of the pressure
        // check for no reason.
        let (drain_threshold, drain_interval) =
            tx_drain_hysteresis(config.rings.completion, frame_count);

        Ok(OpenedLiveXdp {
            rx_pool,
            tx_pool,
            state: Self {
                raw,
                umem: Rc::clone(&umem),
                rx_reclaim,
                first_tx_frame_addr,
                program: Some(program),
                bound_port,
                numa_node,
                pending_fill_scratch: Vec::with_capacity(config.rings.fill as usize),
                rx_descs: Vec::with_capacity(config.rings.rx as usize),
                tx_descs: Vec::with_capacity(config.rings.tx as usize),
                tx_in_flight: 0,
                tx_since_completion_drain: 0,
                tx_completion_drain_threshold: drain_threshold,
                tx_completion_drain_interval: drain_interval,
                tx_wake_pending: false,
            },
        })
    }

    /// Opens `queues.len()` member sockets over **one shared UMEM**.
    ///
    /// Member 0 registers the UMEM (`XDP_UMEM_REG`) and binds normally; members
    /// 1..N bind it with `XDP_SHARED_UMEM` against member 0's fd. The UMEM is
    /// partitioned into one disjoint frame slice per member, so each member runs
    /// the proven single-queue RX/TX path over its own frames while the
    /// (NUMA-local) DMA region is allocated and registered exactly once. The UDP
    /// destination port, when set, is bound once in the shared program; every
    /// member's queue is registered into `XSKMAP[queue]`.
    ///
    /// `config.frame_count` is the **per-member** frame count; the UMEM holds
    /// `frame_count * queues.len()` frames (rounded up to a power of two for the
    /// allocator, with the slack left untouched).
    fn open_shared_members(
        config: &XdpIpPacketSocketConfig,
        queues: &[QueueId],
    ) -> std::io::Result<Vec<OpenedLiveXdp>> {
        assert!(!queues.is_empty(), "open_shared_members requires >=1 queue");
        // Reject duplicate queue ids: each member binds its own AF_XDP socket to
        // `(ifindex, queue)` and registers into `XSKMAP[queue]`. A repeated queue
        // would clobber the earlier member's XSKMAP entry and the second bind on
        // the same queue would later fail with EBUSY — after the UMEM and member
        // 0 are already live. Fail fast and cheaply instead.
        for (index, queue) in queues.iter().enumerate() {
            if queues[..index].contains(queue) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("open_shared_members got duplicate queue id {}", queue.get()),
                ));
            }
        }
        let numa_node = resolve_umem_numa_node(config)?;
        let program = match &config.attached_program {
            Some(program) => {
                if program.if_index() != config.ifindex.get() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "attached XDP program if_index {} does not match socket if_index {}",
                            program.if_index(),
                            config.ifindex.get()
                        ),
                    ));
                }
                program.clone()
            }
            None => XdpProgramHandle::load(
                config.ifindex.get(),
                config.attach_mode,
                config.program_bytes,
            )?,
        };

        let frame_size = umem_frame_size(config)?;
        let per_member = config.frame_count;
        if per_member < 2 || !per_member.is_power_of_two() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("XDP frame_count must be a power of two >= 2 (got {per_member})"),
            ));
        }
        let member_count = queues.len();
        let total_frames =
            u32::try_from(per_member as u64 * member_count as u64).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "shared UMEM frame count overflows u32",
                )
            })?;
        let umem_frames = total_frames.next_power_of_two();
        let per_rx = per_member / 2;
        let umem_headroom = config
            .buffers
            .rx
            .l2_headroom()
            .try_into()
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;

        let mut umem =
            Umem::new_on_numa_node(frame_size, umem_frames, config.huge_page_size, numa_node)?;

        // Member 0 registers the UMEM; members 1..N share it. Each member's
        // FILL ring is prefilled from its own disjoint frame slice.
        let mut raws: Vec<RawXdpSocket> = Vec::with_capacity(member_count);
        let prefill0: Vec<u64> = (0..per_rx).map(|i| umem.frame_offset(i)).collect();
        let raw0 = RawXdpSocket::new_with_umem_headroom(
            config.ifindex.get(),
            queues[0].get(),
            &mut umem,
            config.rings,
            config.mode,
            umem_headroom,
            prefill0,
        )?;
        let owner_fd = raw0.fd();
        raws.push(raw0);
        let umem = Rc::new(umem);
        for (index, queue) in queues.iter().enumerate().skip(1) {
            let base = index as u32 * per_member;
            let prefill: Vec<u64> = (base..base + per_rx)
                .map(|i| umem.frame_offset(i))
                .collect();
            let raw = RawXdpSocket::new_shared_umem(
                config.ifindex.get(),
                queue.get(),
                owner_fd,
                config.rings,
                config.mode,
                prefill,
            )?;
            raws.push(raw);
        }

        // Register every member queue into XSKMAP and bind the UDP port once.
        // Roll back registrations if any step fails before the live states
        // (whose Drop owns cleanup) are built.
        {
            let mut guard = program.lock().expect("XDP program mutex poisoned");
            let mut registered: Vec<u32> = Vec::with_capacity(member_count);
            for (raw, queue) in raws.iter().zip(queues.iter()) {
                if let Err(error) = guard.register_socket(queue.get(), raw.as_fd()) {
                    for done in &registered {
                        let _ = guard.unregister_socket(*done);
                    }
                    return Err(error);
                }
                registered.push(queue.get());
            }
            if let Some(port) = config.bind_udp_port
                && let Err(error) = guard.bind_port(port)
            {
                for done in &registered {
                    let _ = guard.unregister_socket(*done);
                }
                return Err(error);
            }
        }

        // TX completion-drain hysteresis is sized per member (one member's
        // rings/frames), identical across members.
        let (drain_threshold, drain_interval) =
            tx_drain_hysteresis(config.rings.completion, per_member);

        let mut opened = Vec::with_capacity(member_count);
        for (index, raw) in raws.into_iter().enumerate() {
            let base = index as u32 * per_member;
            let first_tx_frame_addr = umem.frame_offset(base + per_rx);
            let tx_frames: Vec<u64> = (base + per_rx..base + per_member)
                .map(|i| umem.frame_offset(i))
                .collect();
            let rx_reclaim = FrameReclaim::new(Vec::with_capacity(per_rx as usize));
            let tx_reclaim = FrameReclaim::new(tx_frames);
            let rx_pool =
                XdpRxPool::live(config.buffers.rx, Rc::clone(&umem), Rc::clone(&rx_reclaim));
            let tx_pool =
                XdpTxPool::live(config.buffers.tx, Rc::clone(&umem), Rc::clone(&tx_reclaim));
            // Only member 0 records the bound port, so exactly one Drop unbinds
            // it from the shared program; every member unregisters its queue.
            let bound_port = if index == 0 {
                config.bind_udp_port
            } else {
                None
            };
            opened.push(OpenedLiveXdp {
                rx_pool,
                tx_pool,
                state: Self {
                    raw,
                    umem: Rc::clone(&umem),
                    rx_reclaim,
                    first_tx_frame_addr,
                    program: Some(program.clone()),
                    bound_port,
                    numa_node,
                    pending_fill_scratch: Vec::with_capacity(config.rings.fill as usize),
                    rx_descs: Vec::with_capacity(config.rings.rx as usize),
                    tx_descs: Vec::with_capacity(config.rings.tx as usize),
                    tx_in_flight: 0,
                    tx_since_completion_drain: 0,
                    tx_completion_drain_threshold: drain_threshold,
                    tx_completion_drain_interval: drain_interval,
                    tx_wake_pending: false,
                },
            });
        }
        Ok(opened)
    }

    fn replenish_fill(&mut self) -> std::io::Result<()> {
        // Drain whatever is ready to be returned to the FILL ring (may be zero
        // if no recv has consumed a frame since the last call).
        let written = if !self.rx_reclaim.is_empty() {
            self.pending_fill_scratch.clear();
            self.rx_reclaim.drain_into(&mut self.pending_fill_scratch);
            let written = self.raw.replenish_fill_batch(&self.pending_fill_scratch);
            if written < self.pending_fill_scratch.len() {
                for addr in self.pending_fill_scratch[written..].iter().copied() {
                    self.rx_reclaim.push(addr);
                }
            }
            written
        } else {
            0
        };

        // Honor `NEED_WAKEUP` regardless of whether we just wrote new frames.
        // The kernel can assert the flag asynchronously when it stalls waiting
        // for FILL entries; gating wakeup on `written > 0` would silently
        // ignore that signal whenever the local reclaim list is empty and
        // leave RX permanently parked.
        if self.raw.fill_needs_wakeup() {
            self.raw.wake_rx()?;
        }
        let _ = written;
        Ok(())
    }
}

/// Computes the cached TX completion-drain `(threshold, interval)` hysteresis
/// pair for one socket. `completion_ring` is the COMPLETION ring depth and
/// `frames_per_socket` is that socket's total UMEM frame count (its TX pool is
/// half of it). Shared by the owned-UMEM `open` and the shared-UMEM
/// `open_shared_members` so the two cannot drift.
fn tx_drain_hysteresis(completion_ring: u32, frames_per_socket: u32) -> (usize, usize) {
    let tx_frame_count = (frames_per_socket / 2) as usize;
    let completion_threshold = ((completion_ring as usize) / 2).max(1);
    let frame_threshold = (tx_frame_count / 2).max(1);
    let drain_threshold = completion_threshold.min(frame_threshold);
    let drain_interval = (drain_threshold / 2).max(1);
    (drain_threshold, drain_interval)
}

fn resolve_umem_numa_node(config: &XdpIpPacketSocketConfig) -> std::io::Result<NumaNode> {
    let iface = if_index_to_name(config.ifindex)?;
    match numa_node_for_interface(&iface) {
        Ok(node) => {
            if let Some(configured) = config.numa_node
                && configured != node
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "configured NUMA node {} does not match {iface} NUMA node {}",
                        configured.get(),
                        node.get()
                    ),
                ));
            }
            Ok(node)
        }
        Err(error) => match config.numa_node {
            Some(node) => Ok(node),
            None => Err(std::io::Error::new(
                error.kind(),
                format!(
                    "could not determine NUMA node for interface {iface}; \
                     set XdpIpPacketSocketBuilder::numa_node only when this \
                     system cannot expose /sys/class/net/{iface}/device/numa_node: {error}"
                ),
            )),
        },
    }
}

impl XdpIpPacketSocket<BusyPollDriver> {
    /// Creates a test-only first-pass busy-poll XDP IP packet socket from
    /// config. Heap-backed pools, no AF_XDP bind — useful for unit tests on
    /// platforms without `CAP_BPF` / `CAP_NET_ADMIN`. Production callers
    /// must use [`Self::new_busy_poll`] (which binds AF_XDP).
    #[cfg(test)]
    #[must_use]
    pub fn new_busy_poll_test(config: XdpIpPacketSocketConfig) -> Self {
        Self::with_driver_test(config, BusyPollDriver::new())
    }

    /// Creates a busy-poll AF_XDP IP packet socket from config.
    pub fn new_busy_poll(config: XdpIpPacketSocketConfig) -> std::io::Result<Self> {
        Self::with_driver(config, BusyPollDriver::new())
    }

    /// Opens one busy-poll member socket per queue over a single shared UMEM.
    ///
    /// All members share one NUMA-local UMEM (member 0 registers it; the rest
    /// bind it with `XDP_SHARED_UMEM`) and one attached program, each over a
    /// disjoint frame slice. Returned in `queues` order; wrap in
    /// [`XdpIpPacketAggregate`](crate::XdpIpPacketAggregate) for one logical
    /// socket. `config.frame_count` is the per-member frame count.
    pub fn open_shared_busy_poll(
        config: XdpIpPacketSocketConfig,
        queues: &[QueueId],
    ) -> std::io::Result<Vec<Self>> {
        let opened = LiveXdpState::open_shared_members(&config, queues)?;
        let mut sockets = Vec::with_capacity(opened.len());
        for (member, queue) in opened.into_iter().zip(queues.iter()) {
            let mut member_config = config.clone();
            member_config.queue_id = *queue;
            let mut socket = construct_state(member_config, BusyPollDriver::new());
            socket.rx_pool = member.rx_pool;
            socket.tx_pool = member.tx_pool;
            socket.live = Some(member.state);
            sockets.push(socket);
        }
        Ok(sockets)
    }
}

impl XdpIpPacketSocket<ReadinessDriver<XdpReadinessSource>> {
    /// Creates a readiness-driven AF_XDP IP packet socket from config.
    pub fn new_readiness(config: XdpIpPacketSocketConfig) -> std::io::Result<Self> {
        let opened = LiveXdpState::open(&config)?;
        let fd = opened.state.raw.try_clone_fd()?;
        let driver = ReadinessDriver::new(XdpReadinessSource::new(fd));
        let mut socket = construct_state(config, driver);
        socket.rx_pool = opened.rx_pool;
        socket.tx_pool = opened.tx_pool;
        socket.live = Some(opened.state);
        Ok(socket)
    }
}

/// Shape of every TX allocator loop in this crate:
///
/// 1. Try to allocate from the TX pool. If anything came back, continue.
/// 2. Otherwise, if we've already drained once this call, stop — the kernel
///    has nothing more for us right now and the caller should retry later.
/// 3. Otherwise drain TX completions. If the drain yielded nothing, stop too;
///    a non-zero drain means new frames will be available next iteration.
///
/// Both [`XdpIpPacketSocket::allocate_tx_batch_inner`] and
/// [`XdpIpPacketSocket::allocate_tx_batch_test`] implement this pattern;
/// the UDP wrapper delegates rather than duplicating it.
#[doc(hidden)]
const _DRAIN_RETRY_ALLOC_DOC: () = ();

/// Hard cap on the test-only `pending_tx_frames` deque. The non-live path
/// is `cfg(test)`-only and lives in `XdpIpPacketSocket` as scaffolding, but
/// in case a future change re-exposes it, this cap stops a stuck or never-
/// draining test from exhausting host memory.
#[cfg(test)]
const MAX_PENDING_TX_FRAMES: usize = 4096;

/// Builds the per-socket state with heap-backed pools and no AF_XDP bind.
/// Both the real constructor [`XdpIpPacketSocket::with_driver`] and the
/// test-only [`XdpIpPacketSocket::with_driver_test`] go through here; the
/// former immediately replaces the heap pools with AF_XDP-backed ones, the
/// latter returns the heap state unmodified.
fn construct_state<D>(config: XdpIpPacketSocketConfig, driver: D) -> XdpIpPacketSocket<D> {
    let rx_capacity = config.rings.rx as usize;
    let tx_capacity = config.rings.tx as usize;
    let nic_queues = [config.queue_id];
    XdpIpPacketSocket {
        nic_queues,
        rx_pool: XdpRxPool::with_heap_capacity(config.buffers.rx, rx_capacity),
        tx_pool: XdpTxPool::with_heap_capacity(config.buffers.tx, tx_capacity),
        routes: XdpLocalRoutes::new(config.route_snapshot.clone()),
        driver,
        config,
        live: None,
        pending_rx: VecDeque::with_capacity(rx_capacity),
        pending_tx_frames: VecDeque::with_capacity(tx_capacity),
        stats: RawDeviceStats::default(),
        _not_send: PhantomData,
    }
}

impl<D> XdpIpPacketSocket<D> {
    /// Creates a test-only XDP IP packet socket state with a concrete driver.
    /// Heap-backed pools, no AF_XDP bind. Available only under `cfg(test)`.
    /// Production callers use [`Self::with_driver`].
    #[cfg(test)]
    #[must_use]
    pub(crate) fn with_driver_test(config: XdpIpPacketSocketConfig, driver: D) -> Self {
        construct_state(config, driver)
    }

    /// Creates an AF_XDP-bound socket state with a concrete driver.
    pub fn with_driver(config: XdpIpPacketSocketConfig, driver: D) -> std::io::Result<Self> {
        let mut socket = construct_state(config, driver);
        let opened = LiveXdpState::open(&socket.config)?;
        socket.rx_pool = opened.rx_pool;
        socket.tx_pool = opened.tx_pool;
        socket.live = Some(opened.state);
        Ok(socket)
    }

    fn drain_tx_completions_inner(&mut self) -> Result<usize, Error> {
        let Some(live) = self.live.as_mut() else {
            return Ok(0);
        };

        let first_tx_frame_addr = live.first_tx_frame_addr;
        let frame_size = u64::from(live.umem.frame_size());
        let frame_mask = frame_size - 1;
        let umem_len = u64::from(live.umem.frame_count()) * frame_size;
        // Resolve the two reclaim queues once outside the loop. The TX pool's
        // live half is guaranteed to be `Some` whenever the socket itself is
        // live; both were constructed together in `LiveXdpState::open`.
        let rx_reclaim: &FrameReclaim = live.rx_reclaim.as_ref();
        let tx_reclaim: &FrameReclaim = self
            .tx_pool
            .live_reclaim()
            .expect("tx pool live state must mirror socket live state");

        let completed = live.raw.drain_completion_slices(
            self.config.rings.completion as usize,
            |first, second| -> Result<(), Error> {
                for &addr in first.iter().chain(second.iter()) {
                    let frame_addr = addr & !frame_mask;
                    if frame_addr >= umem_len {
                        return Err(ring_corrupt_error());
                    }
                    if frame_addr < first_tx_frame_addr {
                        rx_reclaim.push_local(frame_addr);
                    } else {
                        tx_reclaim.push_local(frame_addr);
                    }
                }
                Ok(())
            },
        )?;
        // `tx_in_flight` is only mutated on the owner thread (the live socket
        // is `!Send`), so the increment in `send`/`enqueue_tx_batch` and the
        // decrement here serialize naturally. `saturating_sub` is defensive
        // against an unexpected over-completion that should never happen.
        live.tx_in_flight = live.tx_in_flight.saturating_sub(completed);
        live.tx_since_completion_drain = 0;
        Ok(completed)
    }

    fn should_drain_tx_completions(&self) -> bool {
        // Threshold and interval are precomputed at socket construction
        // (see `LiveXdpState::open`); per-call we just compare against
        // the cached values.
        self.live.as_ref().is_some_and(|live| {
            live.tx_in_flight >= live.tx_completion_drain_threshold
                && live.tx_since_completion_drain >= live.tx_completion_drain_interval
        })
    }

    fn drain_completions_if_tx_pressure(&mut self) -> Result<(), Error> {
        if self.should_drain_tx_completions() {
            let _ = self.drain_tx_completions_inner()?;
        }
        Ok(())
    }

    fn allocate_tx_batch_inner(
        &mut self,
        out: &mut Vec<XdpPacketBufMut>,
        max: usize,
    ) -> Result<usize, Error> {
        if max == 0 {
            return Ok(0);
        }

        self.drain_completions_if_tx_pressure()?;

        // See `drain_retry_alloc_doc` for the loop shape used by every TX
        // allocator in this crate (live and non-live, UDP and IP-packet).
        let start_len = out.len();
        let mut drained_after_empty = false;
        while out.len() - start_len < max {
            let remaining = max - (out.len() - start_len);
            let allocated = self.tx_pool.allocate_many(out, remaining);
            if allocated > 0 {
                drained_after_empty = false;
                continue;
            }

            if drained_after_empty {
                break;
            }

            if self.drain_tx_completions_inner()? == 0 {
                break;
            }
            drained_after_empty = true;
        }
        Ok(out.len() - start_len)
    }

    /// Test-only equivalent of [`Self::allocate_tx_batch_inner`]: pulls buffers
    /// from the heap-backed `tx_pool`, draining stale completions exactly once
    /// when the pool is exhausted. Centralizes the drain-retry pattern so the
    /// UDP wrapper does not re-implement it; see `_DRAIN_RETRY_ALLOC_DOC` for
    /// the shared invariant.
    #[cfg(test)]
    fn allocate_tx_batch_test(
        &mut self,
        out: &mut Vec<XdpPacketBufMut>,
        max: usize,
    ) -> Result<usize, Error> {
        let start_len = out.len();
        let mut drained_after_empty = false;
        while out.len() - start_len < max {
            if let Some(buffer) = self.tx_pool.allocate() {
                out.push(buffer);
                drained_after_empty = false;
                continue;
            }

            if drained_after_empty {
                break;
            }

            // Inline the non-live drain rather than going through the trait
            // method, which would require an unwanted `D: PollDriver` bound
            // on this inherent impl. Non-live mode just clears the pending
            // queue and reports how many entries it dropped.
            let completed = self.pending_tx_frames.len();
            self.pending_tx_frames.clear();
            if completed == 0 {
                break;
            }
            drained_after_empty = true;
        }
        Ok(out.len() - start_len)
    }

    /// Test-only counterpart of the live AF_XDP send path. Validates each
    /// packet against the queue's egress and MTU, prepends the ethernet
    /// header, and pushes the resulting frame into [`Self::pending_tx_frames`]
    /// so tests can observe the bytes that would have been transmitted.
    #[cfg(test)]
    fn send_test(
        &mut self,
        batch: &mut [TxSlot<IpPacketTransmit<XdpPacketBuf, XdpEgress, V4Only>>],
    ) -> Result<usize, SendError> {
        let mut accepted = 0;
        for slot in batch.iter_mut() {
            let Some(tx) = slot.as_ref() else {
                return Err(SendError {
                    accepted,
                    kind: Error::InvalidBatch,
                });
            };

            if tx.egress.ifindex != self.config.ifindex || tx.egress.queue != self.config.queue_id {
                return Err(SendError {
                    accepted,
                    kind: Error::NoEgressRoute,
                });
            }

            if tx.packet.len() > self.config.mtu || tx.packet.len() > tx.egress.mtu as usize {
                self.stats.dropped_oversize = self.stats.dropped_oversize.saturating_add(1);
                return Err(SendError {
                    accepted,
                    kind: Error::OversizeForMtu,
                });
            }

            if !valid_tx_datagram(tx.packet.as_slice(), tx.egress.ethertype) {
                return Err(SendError {
                    accepted,
                    kind: Error::InvalidPacket,
                });
            }

            if tx.packet.headroom() < ethernet_header_len(tx.egress) {
                return Err(SendError {
                    accepted,
                    kind: Error::Device(DeviceError::new(DeviceErrorKind::Backend)),
                });
            }

            let Some(tx) = slot.take() else {
                return Err(SendError {
                    accepted,
                    kind: Error::InvalidBatch,
                });
            };
            if self.pending_tx_frames.len() >= MAX_PENDING_TX_FRAMES {
                // See MAX_PENDING_TX_FRAMES — test-only cap so a never-
                // draining test or adapter doesn't exhaust host memory.
                return Ok(accepted);
            }
            let mut frame = tx.packet.into_mut();
            prepend_ethernet_header(&mut frame, tx.egress);
            self.stats.tx_packets = self.stats.tx_packets.saturating_add(1);
            self.stats.tx_bytes = self
                .stats
                .tx_bytes
                .saturating_add(frame.len().saturating_sub(ethernet_header_len(tx.egress)) as u64);
            self.pending_tx_frames.push_back(frame.freeze());
            accepted += 1;
        }
        Ok(accepted)
    }

    /// Test-only counterpart of the live AF_XDP recv path. Drains the
    /// in-memory [`Self::pending_rx`] queue populated by
    /// [`Self::push_received_ip_packet`] / [`Self::push_received_ethernet_packet`].
    #[cfg(test)]
    fn recv_test(
        &mut self,
        out: &mut RecvBatch<IpPacketReceive<XdpPacketBufMut, XdpIpPacketRecvMeta>>,
    ) -> Result<usize, Error> {
        let mut delivered = 0;
        while out.remaining() > 0 {
            let Some(packet) = self.pending_rx.pop_front() else {
                break;
            };
            self.stats.rx_packets = self.stats.rx_packets.saturating_add(1);
            self.stats.rx_bytes = self
                .stats
                .rx_bytes
                .saturating_add(packet.packet.len() as u64);
            out.push(packet).map_err(|_| Error::BatchFull)?;
            delivered += 1;
        }
        Ok(delivered)
    }

    /// Test-only counterpart of the live AF_XDP completion drain. There is
    /// no real TX ring to drain in this mode, so every accepted packet is
    /// treated as immediately completed: report the pending queue length
    /// and clear it.
    #[cfg(test)]
    fn drain_tx_completions_test(&mut self) -> Result<usize, Error> {
        let completed = self.pending_tx_frames.len();
        self.pending_tx_frames.clear();
        Ok(completed)
    }

    /// Returns queue-local route state.
    #[must_use]
    pub fn routes(&self) -> &XdpLocalRoutes {
        &self.routes
    }

    /// Returns mutable queue-local route state for cold-path update adoption.
    #[must_use]
    pub fn routes_mut(&mut self) -> &mut XdpLocalRoutes {
        &mut self.routes
    }

    /// Applies queued route snapshot updates outside the packet path.
    pub fn apply_route_updates(&mut self) -> usize {
        self.routes.apply_updates()
    }

    /// Pushes a received IP packet into the first-pass in-memory RX queue.
    ///
    /// Real AF_XDP RX ring integration will replace this queue; keeping this
    /// method makes the trait implementation testable without privileges.
    pub fn push_received_ip_packet(&mut self, mut packet: XdpPacketBufMut) -> bool {
        let Some(parsed) = parse_ip_datagram(packet.as_slice()) else {
            return false;
        };
        if parsed.is_fragment {
            self.stats.dropped_fragments = self.stats.dropped_fragments.saturating_add(1);
            return false;
        }
        if packet.len() > parsed.len {
            let trailer_len = packet.len() - parsed.len;
            if packet.trim_suffix(trailer_len).is_err() {
                return false;
            }
        }
        let len = packet.len();
        self.pending_rx.push_back(IpPacketReceive::new(
            packet,
            IpPacketRecvMeta {
                version: parsed.version,
                len,
                checksum: ChecksumStatus::NotChecked,
            },
        ));
        true
    }

    /// Pushes a received Ethernet frame into the first-pass in-memory RX queue.
    ///
    /// The queued packet starts at the IP header, matching the `IpPacketSocket`
    /// boundary. Non-IP frames and IP fragments are silently dropped.
    pub fn push_received_ethernet_packet(&mut self, mut frame: XdpPacketBufMut) -> bool {
        let Some(parsed) = parse_ethernet_frame(frame.as_slice()) else {
            return false;
        };
        if parsed.ip.is_fragment {
            self.stats.dropped_fragments = self.stats.dropped_fragments.saturating_add(1);
            return false;
        }
        let total_len = parsed.l2_len + parsed.ip.len;
        if frame.len() > total_len {
            let trailer_len = frame.len() - total_len;
            if frame.trim_suffix(trailer_len).is_err() {
                return false;
            }
        }
        if frame.trim_prefix(parsed.l2_len).is_err() {
            return false;
        }
        self.push_received_ip_packet(frame)
    }

    /// Copies an Ethernet frame into an RX buffer and normalizes it.
    ///
    /// This is a test and bring-up helper. Real AF_XDP RX will wrap UMEM frames
    /// directly and trim the Ethernet prefix in place.
    pub fn push_received_ethernet_frame(&mut self, frame: &[u8]) -> bool {
        let Some(mut buffer) = self.rx_pool.allocate() else {
            self.stats.ring_full = self.stats.ring_full.saturating_add(1);
            return false;
        };
        if buffer.extend_from_slice(frame).is_err() {
            self.stats.dropped_oversize = self.stats.dropped_oversize.saturating_add(1);
            return false;
        }
        self.push_received_ethernet_packet(buffer)
    }

    /// Returns the number of first-pass TX frames waiting for completion drain.
    #[must_use]
    pub fn pending_tx_frame_count(&self) -> usize {
        self.pending_tx_frames.len()
    }

    /// Returns a submitted Ethernet frame from the first-pass TX queue.
    #[must_use]
    pub fn pending_tx_frame(&self, index: usize) -> Option<&[u8]> {
        self.pending_tx_frames
            .get(index)
            .map(XdpPacketBuf::as_slice)
    }

    fn send_inner(
        &mut self,
        batch: &mut [TxSlot<IpPacketTransmit<XdpPacketBuf, XdpEgress, V4Only>>],
    ) -> Result<usize, SendError> {
        let ifindex = self.config.ifindex;
        let queue_id = self.config.queue_id;
        let mtu = self.config.mtu;
        let mut deferred_error = None;
        let mut prepared = 0usize;
        let accepted;

        if let Err(kind) = self.drain_completions_if_tx_pressure() {
            return Err(SendError { accepted: 0, kind });
        }

        let mut tx_available = self
            .live
            .as_mut()
            .expect("send_inner called only for live socket")
            .raw
            .tx_available() as usize;
        if tx_available == 0 {
            self.stats.ring_full = self.stats.ring_full.saturating_add(1);
            if let Err(kind) = self.drain_tx_completions_inner() {
                return Err(SendError { accepted: 0, kind });
            }
            tx_available = self
                .live
                .as_mut()
                .expect("send_inner called only for live socket")
                .raw
                .tx_available() as usize;
            if tx_available == 0 {
                return Ok(0);
            }
        }

        {
            let live = self
                .live
                .as_mut()
                .expect("send_inner called only for live socket");
            let limit = batch.len().min(tx_available);
            live.tx_descs.clear();
            for slot in batch.iter_mut().take(limit) {
                let Some(tx) = slot.as_mut() else {
                    deferred_error = Some(Error::InvalidBatch);
                    break;
                };

                if let Err(kind) = validate_xdp_ip_transmit(ifindex, queue_id, mtu, tx) {
                    if matches!(&kind, Error::OversizeForMtu) {
                        self.stats.dropped_oversize = self.stats.dropped_oversize.saturating_add(1);
                    }
                    deferred_error = Some(kind);
                    break;
                }

                let l2_len = ethernet_header_len(tx.egress);
                let mut header = [0u8; VLAN_HEADER_LEN];
                write_ethernet_header(&mut header[..l2_len], tx.egress);
                let Some(frame) = tx.packet.prepare_l2(&header[..l2_len]) else {
                    deferred_error =
                        Some(Error::Device(DeviceError::new(DeviceErrorKind::Backend)));
                    break;
                };

                live.tx_descs.push(XdpDesc {
                    addr: frame.desc_addr,
                    len: frame.len,
                    options: 0,
                });
                prepared += 1;
            }

            if prepared == 0 {
                if let Some(kind) = deferred_error {
                    return Err(SendError { accepted: 0, kind });
                }
                return Ok(0);
            }

            accepted = live.raw.enqueue_tx_batch(&live.tx_descs[..prepared]);
            // `prepared` is bounded by the available TX ring space reserved
            // above, so enqueue must accept the whole prepared slice. A short
            // accept would silently drop committed descriptors and leak their
            // frames; surface that as a hard invariant violation.
            assert_eq!(
                accepted, prepared,
                "AF_XDP TX ring accepted fewer descriptors than reserved",
            );
            if accepted > 0 {
                live.raw.commit_tx();
                live.tx_in_flight = live.tx_in_flight.saturating_add(accepted);
                live.tx_since_completion_drain =
                    live.tx_since_completion_drain.saturating_add(accepted);
            }
        }

        for slot in batch.iter_mut().take(accepted) {
            let Some(tx) = slot.take() else {
                return Err(SendError {
                    accepted,
                    kind: Error::InvalidBatch,
                });
            };
            let packet_len = tx.packet.len();
            tx.packet.into_submitted();
            self.stats.tx_packets = self.stats.tx_packets.saturating_add(1);
            self.stats.tx_bytes = self.stats.tx_bytes.saturating_add(packet_len as u64);
        }

        // Skip the sendto doorbell when the kernel hasn't asked for it.
        // The kernel sets `XDP_RING_NEED_WAKEUP` on `tx_prod->flags`
        // whenever it parks the TX poll and needs userspace to kick it
        // back; while the flag stays clear, the kernel is actively
        // draining the TX ring and an extra sendto is pure syscall
        // overhead. There is a small race where the kernel can set the
        // flag *between* our check and the next publish, but a sustained
        // sender re-evaluates the gate on the next batch and recovers
        // within one iteration. End-of-stream callers must flush
        // explicitly via [`UdpSocket::notify_tx`], which still issues an
        // unconditional doorbell.
        //
        // Always retry a pending wake from a prior failure; that path is
        // already off the fast lane.
        let live_needs_wakeup = self
            .live
            .as_ref()
            .is_some_and(|live| live.raw.tx_needs_wakeup());
        let pending_retry = self.live.as_ref().is_some_and(|live| live.tx_wake_pending);
        let needs_wake = pending_retry || (accepted > 0 && live_needs_wakeup);
        if needs_wake {
            let live = self
                .live
                .as_mut()
                .expect("send_inner called only for live socket");
            match live.raw.wake_tx() {
                Ok(()) => live.tx_wake_pending = false,
                Err(error) => {
                    live.tx_wake_pending = true;
                    return Err(SendError {
                        accepted,
                        kind: device_error(error),
                    });
                }
            }
        }

        // See send_udp_inner: post-send drain is redundant with the pre-drain
        // in the next allocate/send iteration.

        if accepted == prepared
            && let Some(kind) = deferred_error
        {
            return Err(SendError { accepted, kind });
        }

        Ok(accepted)
    }

    fn recv_inner(
        &mut self,
        out: &mut RecvBatch<IpPacketReceive<XdpPacketBufMut, XdpIpPacketRecvMeta>>,
    ) -> Result<usize, Error> {
        let live = self
            .live
            .as_mut()
            .expect("recv_inner called only for live socket");
        live.replenish_fill().map_err(device_error)?;

        live.rx_descs.clear();
        let drained = live.raw.drain_rx(&mut live.rx_descs, out.remaining());
        if drained == 0 {
            return Ok(0);
        }

        let mut delivered = 0;
        let rx_reclaim = Rc::clone(&live.rx_reclaim);
        let mut idx = 0;
        while idx < live.rx_descs.len() {
            let desc = live.rx_descs[idx];
            idx += 1;
            let Some((frame_addr, frame)) =
                live.umem.descriptor_slice(desc.addr, desc.len as usize)
            else {
                // Cross-frame or out-of-bounds descriptor. Recover what we can:
                // return the offending frame and every still-pending frame to
                // the FILL pool so a corrupt descriptor does not permanently
                // strip the UMEM rotation.
                reclaim_rx_descs(&rx_reclaim, &live.umem, desc, &live.rx_descs[idx..]);
                live.rx_descs.clear();
                return Err(ring_corrupt_error());
            };
            let Some(parsed) = parse_ethernet_frame(frame) else {
                rx_reclaim.push(frame_addr);
                continue;
            };
            if parsed.ip.is_fragment {
                self.stats.dropped_fragments = self.stats.dropped_fragments.saturating_add(1);
                rx_reclaim.push(frame_addr);
                continue;
            }

            let ip_start = parsed.l2_len;
            let Some(packet) = self
                .rx_pool
                .wrap_rx_frame(desc.addr, ip_start, parsed.ip.len)
            else {
                self.stats.ring_full = self.stats.ring_full.saturating_add(1);
                rx_reclaim.push(frame_addr);
                continue;
            };
            out.push(IpPacketReceive::new(
                packet,
                IpPacketRecvMeta {
                    version: parsed.ip.version,
                    len: parsed.ip.len,
                    checksum: ChecksumStatus::NotChecked,
                },
            ))
            .expect("RecvBatch had remaining capacity reserved before drain_rx");
            self.stats.rx_packets = self.stats.rx_packets.saturating_add(1);
            self.stats.rx_bytes = self.stats.rx_bytes.saturating_add(parsed.ip.len as u64);
            delivered += 1;
        }
        live.rx_descs.clear();

        live.replenish_fill().map_err(device_error)?;
        Ok(delivered)
    }
}

impl XdpUdpSocket<BusyPollDriver, XdpQueueLocalRouter> {
    /// Creates a builder for an AF_XDP UDP socket.
    #[must_use]
    pub fn builder(
        ifindex: IfIndex,
        queue_id: QueueId,
        local_addr: SocketAddrV4,
    ) -> crate::config::XdpUdpSocketBuilder {
        crate::config::XdpUdpSocketBuilder::new(ifindex, queue_id, local_addr)
    }

    /// Opens one busy-poll UDP member socket per queue over a single shared
    /// UMEM (see [`XdpIpPacketSocket::open_shared_busy_poll`]). Each member gets
    /// its own queue-local router seeded from `config.route_snapshot`. Wrap in
    /// [`XdpUdpAggregate`](crate::XdpUdpAggregate) for one logical socket.
    pub fn open_shared_busy_poll(
        config: XdpIpPacketSocketConfig,
        queues: &[QueueId],
        local_addr: SocketAddrV4,
    ) -> std::io::Result<Vec<Self>> {
        let snapshot = config.route_snapshot.clone();
        Self::open_shared_busy_poll_with(config, queues, local_addr, move || {
            XdpQueueLocalRouter::new(snapshot.clone())
        })
    }
}

impl<R> XdpUdpSocket<BusyPollDriver, R> {
    /// Opens one busy-poll UDP member socket per queue over a single shared
    /// UMEM, building each member's router with `make_router` (called once per
    /// member). Lets callers supply a custom [`XdpUdpRouter`].
    pub fn open_shared_busy_poll_with(
        config: XdpIpPacketSocketConfig,
        queues: &[QueueId],
        local_addr: SocketAddrV4,
        mut make_router: impl FnMut() -> R,
    ) -> std::io::Result<Vec<Self>> {
        let ip_members = XdpIpPacketSocket::open_shared_busy_poll(config, queues)?;
        let mut sockets = Vec::with_capacity(ip_members.len());
        for ip in ip_members {
            sockets.push(Self::from_ip_socket(ip, local_addr, make_router()));
        }
        Ok(sockets)
    }
}

impl<D, R> XdpUdpSocket<D, R> {
    pub(crate) fn from_ip_socket(
        ip: XdpIpPacketSocket<D>,
        local_addr: SocketAddrV4,
        router: R,
    ) -> Self {
        Self {
            ip,
            local_addr,
            router,
            ttl: 64,
        }
    }

    /// Returns the configured local IPv4 UDP address.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddrV4 {
        self.local_addr
    }

    /// Returns the UDP router.
    #[must_use]
    pub const fn router(&self) -> &R {
        &self.router
    }

    /// Returns the UDP router mutably.
    #[must_use]
    pub fn router_mut(&mut self) -> &mut R {
        &mut self.router
    }

    /// Returns the IPv4 TTL used for transmitted UDP datagrams.
    #[must_use]
    pub const fn ttl(&self) -> u8 {
        self.ttl
    }

    /// Returns this socket with a different IPv4 TTL.
    #[must_use]
    pub const fn with_ttl(mut self, ttl: u8) -> Self {
        self.ttl = ttl;
        self
    }

    fn ip_mtu(&self) -> usize {
        self.ip.config.mtu
    }
}

impl<D> XdpUdpSocket<D, XdpQueueLocalRouter> {
    /// Returns queue-local route state.
    #[must_use]
    pub fn routes(&self) -> &XdpLocalRoutes {
        self.router.routes()
    }

    /// Returns queue-local route state mutably for cold-path update adoption.
    #[must_use]
    pub fn routes_mut(&mut self) -> &mut XdpLocalRoutes {
        self.router.routes_mut()
    }

    /// Applies queued route snapshot updates outside the packet path.
    pub fn apply_route_updates(&mut self) -> usize {
        self.router.apply_route_updates()
    }
}

#[cfg(test)]
impl UdpEgressContext {
    fn from_ip_socket<D>(socket: &XdpIpPacketSocket<D>) -> Self {
        Self {
            ifindex: socket.config.ifindex,
            queue_id: socket.config.queue_id,
            mtu: socket.config.mtu,
        }
    }

    fn resolve<R>(&self, router: &R, destination: SocketAddr) -> Result<ResolvedUdpEgress, Error>
    where
        R: XdpUdpRouter,
    {
        let SocketAddr::V4(destination) = destination else {
            return Err(Error::InvalidPacket);
        };
        let resolved = router
            .resolve_udp_egress_resolved(
                *destination.ip(),
                XdpRouteContext {
                    ifindex: self.ifindex,
                    queue: self.queue_id,
                    mtu: self.mtu,
                },
            )
            .ok_or(Error::NoEgressRoute)?;
        let egress = resolved.egress();

        validate_xdp_udp_egress(self.ifindex, self.queue_id, egress)?;
        Ok(ResolvedUdpEgress {
            l2_header: resolved.l2_header_array(),
            l2_len: resolved.l2_len(),
            ip_mtu: self.mtu.min(resolved.mtu() as usize),
        })
    }
}

impl<D, R> XdpUdpSocket<D, R>
where
    D: PollDriver,
    R: XdpUdpRouter,
{
    #[cfg(test)]
    fn send_udp_test(
        &mut self,
        batch: &mut [TxSlot<UdpTransmit<XdpPacketBuf>>],
    ) -> Result<usize, SendError> {
        let mut accepted = 0;
        let local_addr = self.local_addr;
        let ttl = self.ttl;
        let egress_context = UdpEgressContext::from_ip_socket(&self.ip);

        for slot in batch.iter_mut() {
            let Some(tx_ref) = slot.as_ref() else {
                return Err(SendError {
                    accepted,
                    kind: Error::InvalidBatch,
                });
            };
            let resolved = egress_context
                .resolve(&self.router, tx_ref.destination)
                .map_err(|kind| SendError { accepted, kind })?;

            let Some(tx) = slot.take() else {
                return Err(SendError {
                    accepted,
                    kind: Error::InvalidBatch,
                });
            };
            let (packet, context) =
                match build_xdp_udp_transmit(local_addr, ttl, resolved.ip_mtu, tx) {
                    Ok(converted) => converted,
                    Err(error) => {
                        *slot = TxSlot::Ready(*error.tx);
                        return Err(SendError {
                            accepted,
                            kind: error.error,
                        });
                    }
                };

            if packet.headroom() < resolved.l2_len {
                *slot = TxSlot::Ready(
                    restore_xdp_udp_transmit(packet, context)
                        .map_err(|kind| SendError { accepted, kind })?,
                );
                return Err(SendError {
                    accepted,
                    kind: Error::Device(DeviceError::new(DeviceErrorKind::Backend)),
                });
            }

            if self.ip.pending_tx_frames.len() >= MAX_PENDING_TX_FRAMES {
                // Non-live (heap) mode caps its pending queue so a test that
                // never drains can't exhaust host memory. Stop here and let
                // the caller drain before sending more.
                return Ok(accepted);
            }
            let packet_len = packet.len();
            let mut frame = packet.into_mut();
            prepend_l2_header(&mut frame, &resolved.l2_header[..resolved.l2_len]);
            self.ip.stats.tx_packets = self.ip.stats.tx_packets.saturating_add(1);
            self.ip.stats.tx_bytes = self.ip.stats.tx_bytes.saturating_add(packet_len as u64);
            self.ip.pending_tx_frames.push_back(frame.freeze());
            accepted += 1;
        }

        Ok(accepted)
    }

    fn send_udp_inner(
        &mut self,
        batch: &mut [TxSlot<UdpTransmit<XdpPacketBuf>>],
    ) -> Result<usize, SendError> {
        let local_addr = self.local_addr;
        let ttl = self.ttl;
        // Fixed for this socket; built once per batch rather than per packet.
        let route_context = XdpRouteContext {
            ifindex: self.ip.config.ifindex,
            queue: self.ip.config.queue_id,
            mtu: self.ip.config.mtu,
        };

        if let Err(kind) = self.ip.drain_completions_if_tx_pressure() {
            return Err(SendError { accepted: 0, kind });
        }

        let mut tx_available = self
            .ip
            .live
            .as_mut()
            .expect("send_udp_inner called only for live socket")
            .raw
            .tx_available() as usize;
        if tx_available == 0 {
            self.ip.stats.ring_full = self.ip.stats.ring_full.saturating_add(1);
            if let Err(kind) = self.ip.drain_tx_completions_inner() {
                return Err(SendError { accepted: 0, kind });
            }
            tx_available = self
                .ip
                .live
                .as_mut()
                .expect("send_udp_inner called only for live socket")
                .raw
                .tx_available() as usize;
            if tx_available == 0 {
                return Ok(0);
            }
        }

        let mut deferred_error = None;
        let accepted;
        let prepared;
        let mut tx_bytes = 0u64;
        let mut wake_error = None;

        {
            let router = &self.router;
            let live = self
                .ip
                .live
                .as_mut()
                .expect("send_udp_inner called only for live socket");
            live.tx_descs.clear();

            let limit = batch.len().min(tx_available);
            for slot in batch.iter_mut().take(limit) {
                let Some(tx) = slot.as_mut() else {
                    deferred_error = Some(Error::InvalidBatch);
                    break;
                };
                let SocketAddr::V4(destination) = tx.destination else {
                    deferred_error = Some(Error::InvalidPacket);
                    break;
                };
                let Some(resolved) = router.resolve_udp_l2(*destination.ip(), route_context)
                else {
                    deferred_error = Some(Error::NoEgressRoute);
                    break;
                };

                if let Err(kind) =
                    prepare_xdp_udp_transmit_in_place(local_addr, ttl, resolved.ip_mtu(), tx)
                {
                    deferred_error = Some(kind);
                    break;
                }

                let Some(frame) = tx.packet.prepare_l2(resolved.l2_header())
                else {
                    if let Err(kind) = restore_prepared_xdp_udp_transmit_in_place(tx) {
                        deferred_error = Some(kind);
                        break;
                    }
                    deferred_error =
                        Some(Error::Device(DeviceError::new(DeviceErrorKind::Backend)));
                    break;
                };

                tx_bytes = tx_bytes.saturating_add(tx.packet.len() as u64);
                live.tx_descs.push(XdpDesc {
                    addr: frame.desc_addr,
                    len: frame.len,
                    options: 0,
                });
            }

            prepared = live.tx_descs.len();
            if prepared == 0 {
                if let Some(kind) = deferred_error {
                    return Err(SendError { accepted: 0, kind });
                }
                return Ok(0);
            }

            accepted = live.raw.enqueue_tx_batch(&live.tx_descs[..prepared]);
            // `prepared` is bounded by the reserved TX ring space and nothing
            // produces between the reservation and here (the live socket is
            // `!Send`), so enqueue must accept the whole prepared slice. A short
            // accept would silently drop committed descriptors and leak their
            // frames; surface that as a hard invariant violation (matches
            // `send_inner`).
            assert_eq!(
                accepted, prepared,
                "AF_XDP TX ring accepted fewer descriptors than reserved",
            );
            if accepted > 0 {
                live.raw.commit_tx();
                live.tx_in_flight = live.tx_in_flight.saturating_add(accepted);
                live.tx_since_completion_drain =
                    live.tx_since_completion_drain.saturating_add(accepted);
            }

            for slot in batch.iter_mut().take(accepted) {
                match slot {
                    TxSlot::Ready(tx) => tx.packet.mark_submitted(),
                    TxSlot::Taken => unreachable!("accepted UDP TX slot was already taken"),
                }
                *slot = TxSlot::Taken;
            }

            // Gate the doorbell on `XDP_RING_NEED_WAKEUP`: while the
            // kernel is actively draining the TX ring the flag stays
            // clear and the sendto is pure syscall overhead. See
            // `send_inner` for the race-window / end-of-stream notes; the
            // same reasoning applies here. Always retry a pending wake
            // left over from a prior failure.
            let needs_wake = live.tx_wake_pending || (accepted > 0 && live.raw.tx_needs_wakeup());
            if needs_wake {
                match live.raw.wake_tx() {
                    Ok(()) => live.tx_wake_pending = false,
                    Err(error) => {
                        live.tx_wake_pending = true;
                        wake_error = Some(error);
                    }
                }
            }
        }

        if accepted > 0 {
            self.ip.stats.tx_packets = self.ip.stats.tx_packets.saturating_add(accepted as u64);
            self.ip.stats.tx_bytes = self.ip.stats.tx_bytes.saturating_add(tx_bytes);
        }

        if let Some(error) = wake_error {
            return Err(SendError {
                accepted,
                kind: device_error(error),
            });
        }

        // Completions accumulated by this send are reclaimed at the top of the
        // next `allocate_tx_batch_inner` (and again at the top of the next
        // `send_udp_inner`); draining here would re-run the same threshold check
        // a few cycles earlier without changing which iteration actually drains.

        if accepted == prepared
            && let Some(kind) = deferred_error
        {
            return Err(SendError { accepted, kind });
        }

        Ok(accepted)
    }

    fn recv_udp(
        &mut self,
        out: &mut RecvBatch<UdpReceive<XdpPacketBufMut, UdpRecvMeta>>,
    ) -> Result<usize, Error> {
        if out.remaining() == 0 {
            return Ok(0);
        }

        #[cfg(test)]
        if self.ip.live.is_none() {
            return self.recv_udp_test(out);
        }
        self.recv_udp_inner(out)
    }

    #[cfg(test)]
    fn recv_udp_test(
        &mut self,
        out: &mut RecvBatch<UdpReceive<XdpPacketBufMut, UdpRecvMeta>>,
    ) -> Result<usize, Error> {
        let mut delivered = 0;
        while out.remaining() > 0 {
            let Some(ip_receive) = self.ip.pending_rx.pop_front() else {
                break;
            };
            self.ip.stats.rx_packets = self.ip.stats.rx_packets.saturating_add(1);
            self.ip.stats.rx_bytes = self
                .ip
                .stats
                .rx_bytes
                .saturating_add(ip_receive.packet.len() as u64);
            let Some(udp) = parse_xdp_udp_receive(self.local_addr, ip_receive)? else {
                continue;
            };
            out.push(udp).map_err(|_| Error::BatchFull)?;
            delivered += 1;
        }

        Ok(delivered)
    }

    fn recv_udp_inner(
        &mut self,
        out: &mut RecvBatch<UdpReceive<XdpPacketBufMut, UdpRecvMeta>>,
    ) -> Result<usize, Error> {
        let live = self
            .ip
            .live
            .as_mut()
            .expect("recv_udp_inner called only for live socket");
        live.replenish_fill().map_err(device_error)?;

        live.rx_descs.clear();
        let drained = live.raw.drain_rx(&mut live.rx_descs, out.remaining());
        if drained == 0 {
            return Ok(0);
        }

        let local_addr = self.local_addr;
        let mut delivered = 0;
        let rx_reclaim = Rc::clone(&live.rx_reclaim);
        let mut idx = 0;
        while idx < live.rx_descs.len() {
            let desc = live.rx_descs[idx];
            idx += 1;
            let Some((frame_addr, frame)) =
                live.umem.descriptor_slice(desc.addr, desc.len as usize)
            else {
                reclaim_rx_descs(&rx_reclaim, &live.umem, desc, &live.rx_descs[idx..]);
                live.rx_descs.clear();
                return Err(ring_corrupt_error());
            };
            let parsed = match parse_ethernet_ipv4_udp(frame) {
                UdpParse::Udp(parsed) => parsed,
                UdpParse::Fragment => {
                    self.ip.stats.dropped_fragments =
                        self.ip.stats.dropped_fragments.saturating_add(1);
                    rx_reclaim.push(frame_addr);
                    continue;
                }
                UdpParse::Drop => {
                    rx_reclaim.push(frame_addr);
                    continue;
                }
            };

            // Accept the packet when the destination port matches AND either
            // the local address is unspecified (`0.0.0.0`, the "any interface
            // address" wildcard) or it equals the parsed destination IP.
            // Without the wildcard branch a socket bound on 0.0.0.0 would
            // reject every packet because the kernel always fills in the
            // real interface address as the L3 destination.
            let local_ip = *local_addr.ip();
            let dest_matches = local_ip == Ipv4Addr::UNSPECIFIED || parsed.destination == local_ip;
            if !dest_matches || parsed.destination_port != local_addr.port() {
                rx_reclaim.push(frame_addr);
                continue;
            }

            let Some(packet) =
                self.ip
                    .rx_pool
                    .wrap_rx_frame(desc.addr, parsed.payload_offset, parsed.payload_len)
            else {
                self.ip.stats.ring_full = self.ip.stats.ring_full.saturating_add(1);
                rx_reclaim.push(frame_addr);
                continue;
            };
            out.push(UdpReceive::new(
                packet,
                UdpRecvMeta {
                    source: SocketAddrV4::new(parsed.source, parsed.source_port).into(),
                    destination: Some(IpAddr::V4(parsed.destination)),
                    ecn: parsed.ecn,
                    len: parsed.payload_len,
                    gro_stride: None,
                },
            ))
            .expect("RecvBatch had remaining capacity reserved before drain_rx");

            self.ip.stats.rx_packets = self.ip.stats.rx_packets.saturating_add(1);
            self.ip.stats.rx_bytes = self.ip.stats.rx_bytes.saturating_add(parsed.ip_len as u64);
            delivered += 1;
        }
        live.rx_descs.clear();

        live.replenish_fill().map_err(device_error)?;
        Ok(delivered)
    }
}

impl<D, R> UdpSocket for XdpUdpSocket<D, R>
where
    D: PollDriver,
    R: XdpUdpRouter,
{
    type RxPool = XdpRxPool;
    type TxPool = XdpTxPool;
    type Driver = D;
    type RecvMeta = UdpRecvMeta;

    fn socket_id(&self) -> SocketId {
        IpPacketSocket::socket_id(&self.ip)
    }

    fn worker_affinity(&self) -> QueueAffinity {
        IpPacketSocket::worker_affinity(&self.ip)
    }

    fn mtu(&self) -> usize {
        self.ip_mtu()
            .saturating_sub(IPV4_MIN_HEADER_LEN + UDP_HEADER_LEN)
    }

    fn capabilities(&self) -> UdpCapabilities {
        UdpCapabilities::default()
    }

    fn rx_pool(&self) -> &Self::RxPool {
        self.ip.rx_pool()
    }

    fn rx_pool_mut(&mut self) -> &mut Self::RxPool {
        self.ip.rx_pool_mut()
    }

    fn tx_pool(&self) -> &Self::TxPool {
        self.ip.tx_pool()
    }

    fn tx_pool_mut(&mut self) -> &mut Self::TxPool {
        self.ip.tx_pool_mut()
    }

    fn allocate_tx_batch(
        &mut self,
        out: &mut Vec<fast_socket_rs::UdpTxBufferMut<Self>>,
        max: usize,
    ) -> Result<usize, Error> {
        #[cfg(test)]
        if self.ip.live.is_none() {
            return self.ip.allocate_tx_batch_test(out, max);
        }
        self.ip.allocate_tx_batch_inner(out, max)
    }

    fn driver(&self) -> &Self::Driver {
        self.ip.driver()
    }

    fn driver_mut(&mut self) -> &mut Self::Driver {
        self.ip.driver_mut()
    }

    fn send(
        &mut self,
        batch: &mut [TxSlot<UdpTransmit<fast_socket_rs::UdpTxBuffer<Self>>>],
    ) -> Result<usize, SendError> {
        #[cfg(test)]
        if self.ip.live.is_none() {
            return self.send_udp_test(batch);
        }
        self.send_udp_inner(batch)
    }

    /// Receives UDP datagrams into `out`.
    ///
    /// The delivered buffers borrow this socket's UMEM through raw pointers:
    /// **this socket must outlive every received buffer** (including any moved
    /// to another thread). Dropping the socket while a received buffer is still
    /// alive is undefined behavior — debug builds panic on later use, release
    /// builds do not check. See [`XdpPacketBufMut`](crate::buffer::XdpPacketBufMut).
    fn recv(
        &mut self,
        out: &mut RecvBatch<UdpReceive<fast_socket_rs::UdpRxBuffer<Self>, Self::RecvMeta>>,
    ) -> Result<usize, Error> {
        self.recv_udp(out)
    }

    fn drain_tx_completions(&mut self) -> Result<usize, Error> {
        self.ip.drain_tx_completions()
    }

    fn notify_tx(&mut self) -> Result<(), Error> {
        self.ip.notify_tx()
    }
}

impl<D> IpPacketSocket for XdpIpPacketSocket<D>
where
    D: PollDriver,
{
    type RxPool = XdpRxPool;
    type TxPool = XdpTxPool;
    type Family = V4Only;
    type Egress = XdpEgress;
    type Driver = D;
    type RecvMeta = XdpIpPacketRecvMeta;

    fn socket_id(&self) -> SocketId {
        SocketId::new(self.config.queue_id.get())
    }

    fn worker_affinity(&self) -> QueueAffinity {
        RawDevice::queue_affinity(self, self.config.queue_id)
    }

    fn mtu(&self) -> usize {
        self.config.mtu
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

    fn send(
        &mut self,
        batch: &mut [TxSlot<IpPacketTransmit<XdpPacketBuf, Self::Egress, Self::Family>>],
    ) -> Result<usize, SendError> {
        #[cfg(test)]
        if self.live.is_none() {
            return self.send_test(batch);
        }
        self.send_inner(batch)
    }

    /// Receives complete IP datagrams into `out`.
    ///
    /// The delivered buffers borrow this socket's UMEM through raw pointers:
    /// **this socket must outlive every received buffer** (including any moved
    /// to another thread). Dropping the socket while a received buffer is still
    /// alive is undefined behavior — debug builds panic on later use, release
    /// builds do not check. See [`XdpPacketBufMut`](crate::buffer::XdpPacketBufMut).
    fn recv(
        &mut self,
        out: &mut RecvBatch<IpPacketReceive<XdpPacketBufMut, Self::RecvMeta>>,
    ) -> Result<usize, Error> {
        #[cfg(test)]
        if self.live.is_none() {
            return self.recv_test(out);
        }
        self.recv_inner(out)
    }

    fn drain_tx_completions(&mut self) -> Result<usize, Error> {
        #[cfg(test)]
        if self.live.is_none() {
            return self.drain_tx_completions_test();
        }
        self.drain_tx_completions_inner()
    }

    fn notify_tx(&mut self) -> Result<(), Error> {
        if let Some(live) = self.live.as_mut() {
            match live.raw.wake_tx() {
                Ok(()) => live.tx_wake_pending = false,
                Err(error) => {
                    live.tx_wake_pending = true;
                    return Err(device_error(error));
                }
            }
        }
        Ok(())
    }
}

impl<D> RawDevice for XdpIpPacketSocket<D>
where
    D: PollDriver,
{
    fn ifindex(&self) -> IfIndex {
        self.config.ifindex
    }

    fn nic_queues(&self) -> &[QueueId] {
        &self.nic_queues
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::NONE
    }

    fn queue_affinity(&self, _queue: QueueId) -> QueueAffinity {
        QueueAffinity::Any
    }

    fn queue_numa_node(&self, _queue: QueueId) -> Option<NumaNode> {
        self.live
            .as_ref()
            .map(|live| live.numa_node)
            .or(self.config.numa_node)
    }

    fn stats(&self, _queue: QueueId) -> RawDeviceStats {
        self.stats
    }

    fn refresh_mtu(&mut self) -> Result<u32, Error> {
        self.config.mtu.try_into().map_err(|error| {
            Error::Device(DeviceError::with_source(DeviceErrorKind::Backend, error))
        })
    }
}

impl<D, R> RawDevice for XdpUdpSocket<D, R>
where
    D: PollDriver,
{
    fn ifindex(&self) -> IfIndex {
        RawDevice::ifindex(&self.ip)
    }

    fn nic_queues(&self) -> &[QueueId] {
        RawDevice::nic_queues(&self.ip)
    }

    fn capabilities(&self) -> Capabilities {
        RawDevice::capabilities(&self.ip)
    }

    fn queue_affinity(&self, queue: QueueId) -> QueueAffinity {
        RawDevice::queue_affinity(&self.ip, queue)
    }

    fn queue_numa_node(&self, queue: QueueId) -> Option<fast_socket_rs::NumaNode> {
        RawDevice::queue_numa_node(&self.ip, queue)
    }

    fn stats(&self, queue: QueueId) -> RawDeviceStats {
        RawDevice::stats(&self.ip, queue)
    }

    fn refresh_mtu(&mut self) -> Result<u32, Error> {
        RawDevice::refresh_mtu(&mut self.ip)
    }
}

#[cfg(test)]
#[derive(Debug)]
struct BuildXdpUdpTransmitError {
    tx: Box<UdpTransmit<XdpPacketBuf>>,
    error: Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ParsedIpv4Udp {
    source: Ipv4Addr,
    destination: Ipv4Addr,
    source_port: u16,
    destination_port: u16,
    ip_len: usize,
    payload_offset: usize,
    payload_len: usize,
    ecn: Option<fast_socket_rs::EcnCodepoint>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ParsedEthernetIpv4Udp {
    source: Ipv4Addr,
    destination: Ipv4Addr,
    source_port: u16,
    destination_port: u16,
    ip_len: usize,
    payload_offset: usize,
    payload_len: usize,
    ecn: Option<fast_socket_rs::EcnCodepoint>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UdpParse<T> {
    Udp(T),
    Fragment,
    Drop,
}

#[derive(Clone, Copy, Debug)]
struct Ipv4UdpHeaderFields {
    source_port: u16,
    destination_port: u16,
    source: Ipv4Addr,
    destination: Ipv4Addr,
    total_len: u16,
    udp_len: u16,
    ttl: u8,
    ecn: Option<fast_socket_rs::EcnCodepoint>,
}

#[cfg(test)]
fn validate_xdp_udp_egress(
    ifindex: IfIndex,
    queue_id: QueueId,
    egress: XdpEgress,
) -> Result<(), Error> {
    if egress.ifindex != ifindex || egress.queue != queue_id {
        return Err(Error::NoEgressRoute);
    }
    if egress.ethertype != ETHERTYPE_IPV4 {
        return Err(Error::InvalidPacket);
    }
    Ok(())
}

fn validate_xdp_ip_transmit(
    ifindex: IfIndex,
    queue_id: QueueId,
    mtu: usize,
    tx: &IpPacketTransmit<XdpPacketBuf, XdpEgress, V4Only>,
) -> Result<(), Error> {
    if tx.egress.ifindex != ifindex || tx.egress.queue != queue_id {
        return Err(Error::NoEgressRoute);
    }

    if tx.packet.len() > mtu || tx.packet.len() > tx.egress.mtu as usize {
        return Err(Error::OversizeForMtu);
    }

    if !valid_tx_datagram(tx.packet.as_slice(), tx.egress.ethertype) {
        return Err(Error::InvalidPacket);
    }

    if tx.packet.headroom() < ethernet_header_len(tx.egress) {
        return Err(Error::Device(DeviceError::new(DeviceErrorKind::Backend)));
    }

    Ok(())
}

fn prepare_xdp_udp_transmit_in_place(
    local_addr: SocketAddrV4,
    ttl: u8,
    ip_mtu: usize,
    tx: &mut UdpTransmit<XdpPacketBuf>,
) -> Result<(), Error> {
    let destination = match tx.destination {
        SocketAddr::V4(addr) => addr,
        SocketAddr::V6(_) => return Err(Error::InvalidPacket),
    };

    let source_ip = match tx.source_ip {
        Some(IpAddr::V4(addr)) => addr,
        Some(IpAddr::V6(_)) => return Err(Error::InvalidPacket),
        None => *local_addr.ip(),
    };

    let payload_len = tx.packet.len();
    let Some(max_payload) = ip_mtu.checked_sub(IPV4_MIN_HEADER_LEN + UDP_HEADER_LEN) else {
        return Err(Error::InvalidPacket);
    };
    if payload_len > max_payload {
        return Err(Error::OversizeForMtu);
    }

    let udp_len = payload_len + UDP_HEADER_LEN;
    let total_len = udp_len + IPV4_MIN_HEADER_LEN;
    if total_len > u16::MAX as usize {
        return Err(Error::OversizeForMtu);
    }

    let mut headers = [0u8; IPV4_MIN_HEADER_LEN + UDP_HEADER_LEN];
    write_ipv4_udp_headers(
        &mut headers,
        Ipv4UdpHeaderFields {
            source_port: local_addr.port(),
            destination_port: destination.port(),
            source: source_ip,
            destination: *destination.ip(),
            total_len: total_len as u16,
            udp_len: udp_len as u16,
            ttl,
            ecn: tx.ecn,
        },
    );

    tx.packet
        .prepend(&headers)
        .map_err(|_| Error::InvalidPacket)
}

fn restore_prepared_xdp_udp_transmit_in_place(
    tx: &mut UdpTransmit<XdpPacketBuf>,
) -> Result<(), Error> {
    tx.packet
        .trim_prefix(IPV4_MIN_HEADER_LEN + UDP_HEADER_LEN)
        .map_err(|_| Error::InvalidPacket)
}

#[cfg(test)]
fn build_xdp_udp_transmit(
    local_addr: SocketAddrV4,
    ttl: u8,
    ip_mtu: usize,
    tx: UdpTransmit<XdpPacketBuf>,
) -> Result<(XdpPacketBuf, XdpUdpTxContext), BuildXdpUdpTransmitError> {
    let context = XdpUdpTxContext {
        destination: tx.destination,
        source_ip: tx.source_ip,
        ecn: tx.ecn,
        gso_segment_size: tx.gso_segment_size,
    };

    let destination = match tx.destination {
        SocketAddr::V4(addr) => addr,
        SocketAddr::V6(_) => return Err(build_xdp_udp_error(tx, Error::InvalidPacket)),
    };

    let source_ip = match tx.source_ip {
        Some(IpAddr::V4(addr)) => addr,
        Some(IpAddr::V6(_)) => return Err(build_xdp_udp_error(tx, Error::InvalidPacket)),
        None => *local_addr.ip(),
    };

    let payload_len = tx.packet.len();
    // The MTU must accommodate the IPv4 and UDP headers we are about to
    // prepend. A misconfigured MTU smaller than just the header overhead is
    // a programmer error, not a runtime back-pressure condition; surface it
    // as a hard `Error::InvalidPacket` (via the build-error path) instead of
    // silently treating every payload as oversize.
    let Some(max_payload) = ip_mtu.checked_sub(IPV4_MIN_HEADER_LEN + UDP_HEADER_LEN) else {
        return Err(build_xdp_udp_error(tx, Error::InvalidPacket));
    };
    if payload_len > max_payload {
        return Err(build_xdp_udp_error(tx, Error::OversizeForMtu));
    }

    let udp_len = payload_len + UDP_HEADER_LEN;
    let total_len = udp_len + IPV4_MIN_HEADER_LEN;
    if total_len > u16::MAX as usize {
        return Err(build_xdp_udp_error(tx, Error::OversizeForMtu));
    }

    let mut packet = tx.packet.into_mut();
    let mut headers = [0u8; IPV4_MIN_HEADER_LEN + UDP_HEADER_LEN];
    write_ipv4_udp_headers(
        &mut headers,
        Ipv4UdpHeaderFields {
            source_port: local_addr.port(),
            destination_port: destination.port(),
            source: source_ip,
            destination: *destination.ip(),
            total_len: total_len as u16,
            udp_len: udp_len as u16,
            ttl,
            ecn: tx.ecn,
        },
    );

    if packet.prepend(&headers).is_err() {
        return Err(BuildXdpUdpTransmitError {
            tx: Box::new(tx_from_xdp_udp_context(packet.freeze(), context)),
            error: Error::InvalidPacket,
        });
    }

    Ok((packet.freeze(), context))
}

#[cfg(test)]
fn build_xdp_udp_error(tx: UdpTransmit<XdpPacketBuf>, error: Error) -> BuildXdpUdpTransmitError {
    BuildXdpUdpTransmitError {
        tx: Box::new(tx),
        error,
    }
}

#[cfg(test)]
fn restore_xdp_udp_transmit(
    packet: XdpPacketBuf,
    context: XdpUdpTxContext,
) -> Result<UdpTransmit<XdpPacketBuf>, Error> {
    let mut packet = packet.into_mut();
    packet
        .trim_prefix(IPV4_MIN_HEADER_LEN + UDP_HEADER_LEN)
        .map_err(|_| Error::InvalidPacket)?;
    Ok(tx_from_xdp_udp_context(packet.freeze(), context))
}

#[cfg(test)]
fn tx_from_xdp_udp_context(
    packet: XdpPacketBuf,
    context: XdpUdpTxContext,
) -> UdpTransmit<XdpPacketBuf> {
    UdpTransmit {
        packet,
        destination: context.destination,
        source_ip: context.source_ip,
        ecn: context.ecn,
        gso_segment_size: context.gso_segment_size,
    }
}

#[cfg(test)]
fn reclaim_completed_xdp_frame(
    frame_addr: u64,
    first_tx_frame_addr: u64,
    rx_reclaim: &FrameReclaim,
    tx_pool: &mut XdpTxPool,
) {
    if frame_addr < first_tx_frame_addr {
        rx_reclaim.push_local(frame_addr);
    } else {
        tx_pool.reclaim_completed_frame(frame_addr);
    }
}

#[cfg(test)]
fn parse_xdp_udp_receive(
    local_addr: SocketAddrV4,
    mut ip: IpPacketReceive<XdpPacketBufMut, XdpIpPacketRecvMeta>,
) -> Result<Option<UdpReceive<XdpPacketBufMut, UdpRecvMeta>>, Error> {
    let Some(parsed) = parse_ipv4_udp(ip.packet.as_slice()) else {
        return Ok(None);
    };

    // See the live-path filter: accept either an exact destination-IP match
    // or any IP when the socket is bound on 0.0.0.0.
    let local_ip = *local_addr.ip();
    let dest_matches = local_ip == Ipv4Addr::UNSPECIFIED || parsed.destination == local_ip;
    if !dest_matches || parsed.destination_port != local_addr.port() {
        return Ok(None);
    }

    let udp_end = parsed.payload_offset + parsed.payload_len;
    if ip.packet.len() > udp_end {
        ip.packet
            .trim_suffix(ip.packet.len() - udp_end)
            .map_err(|_| Error::InvalidPacket)?;
    }

    ip.packet
        .trim_prefix(parsed.payload_offset)
        .map_err(|_| Error::InvalidPacket)?;

    Ok(Some(UdpReceive::new(
        ip.packet,
        UdpRecvMeta {
            source: SocketAddrV4::new(parsed.source, parsed.source_port).into(),
            destination: Some(IpAddr::V4(parsed.destination)),
            ecn: parsed.ecn,
            len: parsed.payload_len,
            gro_stride: None,
        },
    )))
}

#[cfg(test)]
fn parse_ipv4_udp(packet: &[u8]) -> Option<ParsedIpv4Udp> {
    match parse_ipv4_udp_datagram(packet) {
        UdpParse::Udp(parsed) => Some(parsed),
        UdpParse::Fragment | UdpParse::Drop => None,
    }
}

fn parse_ipv4_udp_datagram(packet: &[u8]) -> UdpParse<ParsedIpv4Udp> {
    if packet.len() < IPV4_MIN_HEADER_LEN {
        return UdpParse::Drop;
    }
    if packet[0] >> 4 != 4 {
        return UdpParse::Drop;
    }

    let ihl = usize::from(packet[0] & 0x0f) * 4;
    if ihl < IPV4_MIN_HEADER_LEN || packet.len() < ihl {
        return UdpParse::Drop;
    }

    let total_len = usize::from(u16::from_be_bytes([packet[2], packet[3]]));
    if total_len < ihl || total_len > packet.len() {
        return UdpParse::Drop;
    }

    let fragment = u16::from_be_bytes([packet[6], packet[7]]);
    if (fragment & IPV4_FRAGMENT_MASK) != 0 {
        return UdpParse::Fragment;
    }

    if packet[9] != UDP_PROTOCOL || total_len < ihl + UDP_HEADER_LEN {
        return UdpParse::Drop;
    }

    let udp = &packet[ihl..ihl + UDP_HEADER_LEN];
    let udp_len = usize::from(u16::from_be_bytes([udp[4], udp[5]]));
    if udp_len < UDP_HEADER_LEN || ihl + udp_len > total_len {
        return UdpParse::Drop;
    }

    UdpParse::Udp(ParsedIpv4Udp {
        source: Ipv4Addr::new(packet[12], packet[13], packet[14], packet[15]),
        destination: Ipv4Addr::new(packet[16], packet[17], packet[18], packet[19]),
        source_port: u16::from_be_bytes([udp[0], udp[1]]),
        destination_port: u16::from_be_bytes([udp[2], udp[3]]),
        ip_len: total_len,
        payload_offset: ihl + UDP_HEADER_LEN,
        payload_len: udp_len - UDP_HEADER_LEN,
        ecn: ecn_from_bits(packet[1] & 0x03),
    })
}

fn parse_ethernet_ipv4_udp(frame: &[u8]) -> UdpParse<ParsedEthernetIpv4Udp> {
    if frame.len() < ETHERNET_HEADER_LEN {
        return UdpParse::Drop;
    }

    let mut l2_len = ETHERNET_HEADER_LEN;
    let mut ethertype = u16::from_be_bytes([frame[12], frame[13]]);
    if ethertype == VLAN_ETHERTYPE {
        if frame.len() < VLAN_HEADER_LEN {
            return UdpParse::Drop;
        }
        ethertype = u16::from_be_bytes([frame[16], frame[17]]);
        l2_len = VLAN_HEADER_LEN;
    }

    if ethertype != ETHERTYPE_IPV4 {
        return UdpParse::Drop;
    }

    match parse_ipv4_udp_datagram(&frame[l2_len..]) {
        UdpParse::Udp(parsed) => UdpParse::Udp(ParsedEthernetIpv4Udp {
            source: parsed.source,
            destination: parsed.destination,
            source_port: parsed.source_port,
            destination_port: parsed.destination_port,
            ip_len: parsed.ip_len,
            payload_offset: l2_len + parsed.payload_offset,
            payload_len: parsed.payload_len,
            ecn: parsed.ecn,
        }),
        UdpParse::Fragment => UdpParse::Fragment,
        UdpParse::Drop => UdpParse::Drop,
    }
}

fn write_ipv4_udp_headers(
    headers: &mut [u8; IPV4_MIN_HEADER_LEN + UDP_HEADER_LEN],
    fields: Ipv4UdpHeaderFields,
) {
    headers[0] = 0x45;
    headers[1] = ecn_bits(fields.ecn);
    headers[2..4].copy_from_slice(&fields.total_len.to_be_bytes());
    headers[6..8].copy_from_slice(&0u16.to_be_bytes());
    headers[8] = fields.ttl;
    headers[9] = UDP_PROTOCOL;
    headers[12..16].copy_from_slice(&fields.source.octets());
    headers[16..20].copy_from_slice(&fields.destination.octets());
    let checksum = ipv4_header_checksum(&headers[..IPV4_MIN_HEADER_LEN]);
    headers[10..12].copy_from_slice(&checksum.to_be_bytes());

    headers[20..22].copy_from_slice(&fields.source_port.to_be_bytes());
    headers[22..24].copy_from_slice(&fields.destination_port.to_be_bytes());
    headers[24..26].copy_from_slice(&fields.udp_len.to_be_bytes());
    headers[26..28].copy_from_slice(&0u16.to_be_bytes());
}

fn ipv4_header_checksum(header: &[u8]) -> u16 {
    // IPv4 headers are always a multiple of 4 bytes (IHL is in 32-bit words),
    // so `chunks_exact(2)` consumes the full header with no trailing odd byte.
    debug_assert!(header.len().is_multiple_of(2));
    let mut sum = 0u32;
    for chunk in header.chunks_exact(2) {
        sum += u32::from(u16::from_be_bytes([chunk[0], chunk[1]]));
    }
    while (sum >> 16) != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

fn ecn_bits(ecn: Option<fast_socket_rs::EcnCodepoint>) -> u8 {
    match ecn {
        Some(fast_socket_rs::EcnCodepoint::Ect0) => 0b10,
        Some(fast_socket_rs::EcnCodepoint::Ect1) => 0b01,
        Some(fast_socket_rs::EcnCodepoint::Ce) => 0b11,
        Some(fast_socket_rs::EcnCodepoint::NotEct) | Some(_) | None => 0,
    }
}

fn ecn_from_bits(bits: u8) -> Option<fast_socket_rs::EcnCodepoint> {
    Some(match bits & 0x03 {
        0b00 => fast_socket_rs::EcnCodepoint::NotEct,
        0b01 => fast_socket_rs::EcnCodepoint::Ect1,
        0b10 => fast_socket_rs::EcnCodepoint::Ect0,
        0b11 => fast_socket_rs::EcnCodepoint::Ce,
        _ => unreachable!("masked to two bits"),
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ParsedEthernet {
    l2_len: usize,
    ip: ParsedIp,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ParsedIp {
    version: IpVersion,
    len: usize,
    is_fragment: bool,
}

fn parse_ethernet_frame(frame: &[u8]) -> Option<ParsedEthernet> {
    if frame.len() < ETHERNET_HEADER_LEN {
        return None;
    }

    let mut l2_len = ETHERNET_HEADER_LEN;
    let mut ethertype = u16::from_be_bytes([frame[12], frame[13]]);
    if ethertype == VLAN_ETHERTYPE {
        if frame.len() < VLAN_HEADER_LEN {
            return None;
        }
        ethertype = u16::from_be_bytes([frame[16], frame[17]]);
        l2_len = VLAN_HEADER_LEN;
    }

    if ethertype != ETHERTYPE_IPV4 && ethertype != ETHERTYPE_IPV6 {
        return None;
    }

    let ip = parse_ip_datagram(&frame[l2_len..])?;
    Some(ParsedEthernet { l2_len, ip })
}

fn parse_ip_datagram(packet: &[u8]) -> Option<ParsedIp> {
    match packet.first().map(|byte| byte >> 4) {
        Some(4) => parse_ipv4_datagram(packet),
        Some(6) => parse_ipv6_datagram(packet),
        _ => None,
    }
}

fn parse_ipv4_datagram(packet: &[u8]) -> Option<ParsedIp> {
    if packet.len() < IPV4_MIN_HEADER_LEN {
        return None;
    }
    let ihl = usize::from(packet[0] & 0x0f) * 4;
    if ihl < IPV4_MIN_HEADER_LEN || packet.len() < ihl {
        return None;
    }
    let total_len = usize::from(u16::from_be_bytes([packet[2], packet[3]]));
    if total_len < ihl || total_len > packet.len() {
        return None;
    }
    let fragment = u16::from_be_bytes([packet[6], packet[7]]);
    Some(ParsedIp {
        version: IpVersion::V4,
        len: total_len,
        is_fragment: fragment & IPV4_FRAGMENT_MASK != 0,
    })
}

fn parse_ipv6_datagram(packet: &[u8]) -> Option<ParsedIp> {
    if packet.len() < IPV6_HEADER_LEN {
        return None;
    }
    let payload_len = usize::from(u16::from_be_bytes([packet[4], packet[5]]));
    let total_len = IPV6_HEADER_LEN.checked_add(payload_len)?;
    if total_len > packet.len() {
        return None;
    }
    Some(ParsedIp {
        version: IpVersion::V6,
        len: total_len,
        is_fragment: has_ipv6_fragment_header(&packet[..total_len]),
    })
}

fn has_ipv6_fragment_header(packet: &[u8]) -> bool {
    let mut next = packet[6];
    let mut offset = IPV6_HEADER_LEN;
    loop {
        match next {
            IPV6_NEXT_HEADER_FRAGMENT => return true,
            0 | 43 | 60 => {
                if offset + 2 > packet.len() {
                    return false;
                }
                next = packet[offset];
                let extension_len = (usize::from(packet[offset + 1]) + 1) * 8;
                offset = offset.saturating_add(extension_len);
                if offset > packet.len() {
                    return false;
                }
            }
            51 => {
                if offset + 2 > packet.len() {
                    return false;
                }
                next = packet[offset];
                let extension_len = (usize::from(packet[offset + 1]) + 2) * 4;
                offset = offset.saturating_add(extension_len);
                if offset > packet.len() {
                    return false;
                }
            }
            _ => return false,
        }
    }
}

fn valid_tx_datagram(packet: &[u8], ethertype: u16) -> bool {
    let Some(parsed) = parse_ip_datagram(packet) else {
        return false;
    };
    if parsed.len != packet.len() {
        return false;
    }
    match (ethertype, parsed.version) {
        (ETHERTYPE_IPV4, IpVersion::V4) => !parsed.is_fragment,
        (ETHERTYPE_IPV6, IpVersion::V6) => !parsed.is_fragment,
        _ => false,
    }
}

#[cfg(test)]
fn prepend_ethernet_header(buffer: &mut XdpPacketBufMut, egress: XdpEgress) {
    let mut header = [0u8; VLAN_HEADER_LEN];
    write_ethernet_header(&mut header[..ethernet_header_len(egress)], egress);
    prepend_l2_header(buffer, &header[..ethernet_header_len(egress)]);
}

#[cfg(test)]
fn prepend_l2_header(buffer: &mut XdpPacketBufMut, header: &[u8]) {
    buffer
        .prepend(header)
        .expect("send prevalidated sufficient L2 headroom");
}

/// Derives the AF_XDP UMEM frame size from the configured RX/TX buffer
/// layouts, rounded up to the next power of two as the kernel requires.
fn umem_frame_size(config: &XdpIpPacketSocketConfig) -> std::io::Result<u32> {
    let frame_size = config
        .buffers
        .rx
        .chunk_size()
        .max(config.buffers.tx.chunk_size())
        .checked_next_power_of_two()
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "XDP frame size overflows power-of-two rounding",
            )
        })?;
    u32::try_from(frame_size)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
}

fn device_error(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::Device(DeviceError::with_source(DeviceErrorKind::Backend, error))
}

fn ring_corrupt_error() -> Error {
    Error::Device(DeviceError::new(DeviceErrorKind::RingCorrupt))
}

/// Best-effort reclaim of one offending descriptor plus every still-pending
/// descriptor when an RX iteration is aborting. Frames whose address is not
/// recoverable (e.g. wholly out of bounds) are leaked, but every recoverable
/// frame returns to the FILL pool so corrupt descriptors do not permanently
/// drain the UMEM rotation.
fn reclaim_rx_descs(
    rx_reclaim: &FrameReclaim,
    umem: &Umem,
    failing: XdpDesc,
    remaining: &[XdpDesc],
) {
    if let Some(addr) = umem.frame_addr_for_desc(failing.addr) {
        rx_reclaim.push(addr);
    }
    for desc in remaining {
        if let Some(addr) = umem.frame_addr_for_desc(desc.addr) {
            rx_reclaim.push(addr);
        }
    }
}

fn wait_for_readable(fd: BorrowedFd<'_>, timeout: Option<Duration>) -> Result<WaitOutcome, Error> {
    let mut pollfd = libc::pollfd {
        fd: std::os::fd::AsRawFd::as_raw_fd(&fd),
        events: libc::POLLIN,
        revents: 0,
    };
    let timeout_ms = match timeout {
        None => -1,
        Some(timeout) if timeout.is_zero() => 0,
        Some(timeout) => timeout.as_millis().try_into().unwrap_or(i32::MAX).max(1),
    };
    // SAFETY: pollfd points to a valid single-element pollfd array.
    let rc = unsafe { libc::poll(&mut pollfd, 1, timeout_ms) };
    match rc {
        value if value > 0 => Ok(WaitOutcome::Ready),
        0 => Ok(WaitOutcome::Timeout),
        _ => Err(Error::Device(DeviceError::with_source(
            DeviceErrorKind::Backend,
            std::io::Error::last_os_error(),
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::rc::Rc;

    use fast_socket_rs::{
        BufferLayout, BufferPool, Error, HugePageSize, LinkAddr, PacketBufferMut, UdpSocket,
        UdpTransmit,
    };

    use super::*;
    use crate::config::XdpIpPacketSocketBuilder;
    use crate::route::{InterfaceInfo, Ipv4Route, RouteSnapshot};

    fn egress() -> XdpEgress {
        XdpEgress::ipv4(
            IfIndex::new(1),
            QueueId::new(0),
            LinkAddr::new([1, 2, 3, 4, 5, 6]),
            LinkAddr::new([6, 5, 4, 3, 2, 1]),
            1500,
        )
    }

    fn mac(value: u8) -> LinkAddr {
        LinkAddr::new([value; 6])
    }

    fn route_snapshot_for_gateway(
        ifindex: IfIndex,
        queue: QueueId,
        gateway: Ipv4Addr,
        dst_mac: LinkAddr,
        src_mac: LinkAddr,
    ) -> RouteSnapshot {
        let mut snapshot = RouteSnapshot::new();
        snapshot.upsert_interface(InterfaceInfo {
            ifindex,
            master_ifindex: None,
            mac: src_mac,
            mtu: 1500,
            queue,
        });
        snapshot.upsert_route_v4(Ipv4Route {
            destination: Ipv4Addr::UNSPECIFIED,
            prefix_len: 0,
            ifindex,
            gateway: Some(gateway),
            priority: 100,
            mtu: 1500,
        });
        snapshot.upsert_neighbor_v4(ifindex, gateway, dst_mac);
        snapshot
    }

    #[derive(Clone, Copy, Debug)]
    struct StaticUdpRouter {
        egress: XdpEgress,
    }

    impl XdpUdpRouter for StaticUdpRouter {
        fn resolve_udp_egress(
            &self,
            _dst: Ipv4Addr,
            context: XdpRouteContext,
        ) -> Option<XdpEgress> {
            (context.ifindex == self.egress.ifindex && context.queue == self.egress.queue)
                .then_some(self.egress)
        }
    }

    fn ipv4_packet() -> [u8; 20] {
        [
            0x45, 0, 0, 20, 0, 0, 0, 0, 64, 17, 0, 0, 192, 0, 2, 1, 198, 51, 100, 1,
        ]
    }

    fn ethernet_frame(ip: &[u8]) -> Vec<u8> {
        let mut frame = Vec::new();
        frame.extend_from_slice(&[1, 2, 3, 4, 5, 6]);
        frame.extend_from_slice(&[6, 5, 4, 3, 2, 1]);
        frame.extend_from_slice(&ETHERTYPE_IPV4.to_be_bytes());
        frame.extend_from_slice(ip);
        frame
    }

    fn ipv4_udp_packet(
        source: Ipv4Addr,
        destination: Ipv4Addr,
        source_port: u16,
        destination_port: u16,
        payload: &[u8],
    ) -> Vec<u8> {
        let udp_len = UDP_HEADER_LEN + payload.len();
        let total_len = IPV4_MIN_HEADER_LEN + udp_len;
        let mut headers = [0u8; IPV4_MIN_HEADER_LEN + UDP_HEADER_LEN];
        write_ipv4_udp_headers(
            &mut headers,
            Ipv4UdpHeaderFields {
                source_port,
                destination_port,
                source,
                destination,
                total_len: total_len as u16,
                udp_len: udp_len as u16,
                ttl: 64,
                ecn: None,
            },
        );

        let mut packet = Vec::with_capacity(total_len);
        packet.extend_from_slice(&headers);
        packet.extend_from_slice(payload);
        packet
    }

    fn live_layout() -> BufferLayout {
        BufferLayout::with_headroom_and_tailroom(128, 64, 0)
            .with_l2_headroom(64)
            .with_alignment(NonZeroUsize::new(2048).unwrap())
            .with_fixed_chunk(2048, 2048)
    }

    #[test]
    fn live_completion_reclaim_preserves_rx_and_tx_frame_pools() {
        let umem = Rc::new(Umem::new(2048, 4, HugePageSize::Default).unwrap());
        let rx_reclaim = FrameReclaim::new(Vec::new());
        let tx_reclaim = FrameReclaim::new(Vec::new());
        let mut tx_pool = XdpTxPool::live(live_layout(), Rc::clone(&umem), Rc::clone(&tx_reclaim));
        let first_tx_frame_addr = umem.frame_offset(2);

        reclaim_completed_xdp_frame(
            umem.frame_addr_for_desc(umem.frame_offset(1) + 128)
                .unwrap(),
            first_tx_frame_addr,
            &rx_reclaim,
            &mut tx_pool,
        );
        reclaim_completed_xdp_frame(
            umem.frame_addr_for_desc(umem.frame_offset(3) + 64).unwrap(),
            first_tx_frame_addr,
            &rx_reclaim,
            &mut tx_pool,
        );

        let mut rx_frames = Vec::new();
        rx_reclaim.drain_into(&mut rx_frames);
        assert_eq!(rx_frames, vec![umem.frame_offset(1)]);

        let mut tx_frames = Vec::new();
        tx_reclaim.drain_into(&mut tx_frames);
        assert_eq!(tx_frames, vec![umem.frame_offset(3)]);
    }

    #[test]
    fn xdp_ip_packet_socket_accepts_static_egress_send() {
        let mut socket =
            XdpIpPacketSocketBuilder::new(IfIndex::new(1), QueueId::new(0)).open_busy_poll_test();
        let ip = ipv4_packet();
        let mut packet = socket.tx_pool_mut().allocate().unwrap();
        packet.extend_from_slice(&ip).unwrap();
        let mut batch = [TxSlot::Ready(IpPacketTransmit::new(
            packet.freeze(),
            egress(),
        ))];

        assert_eq!(socket.send(&mut batch).unwrap(), 1);
        assert!(batch[0].is_taken());
        assert_eq!(socket.stats(QueueId::new(0)).tx_packets, 1);
        assert_eq!(socket.pending_tx_frame_count(), 1);

        let frame = socket.pending_tx_frame(0).unwrap();
        assert_eq!(&frame[..6], &[1, 2, 3, 4, 5, 6]);
        assert_eq!(&frame[6..12], &[6, 5, 4, 3, 2, 1]);
        assert_eq!(&frame[12..14], &ETHERTYPE_IPV4.to_be_bytes());
        assert_eq!(&frame[14..], &ip);
        assert_eq!(socket.drain_tx_completions().unwrap(), 1);
        assert_eq!(socket.pending_tx_frame_count(), 0);
    }

    #[test]
    fn xdp_ip_packet_socket_normalizes_ethernet_rx_to_ip_packet() {
        let mut socket =
            XdpIpPacketSocketBuilder::new(IfIndex::new(1), QueueId::new(0)).open_busy_poll_test();
        let ip = ipv4_packet();
        assert!(socket.push_received_ethernet_frame(&ethernet_frame(&ip)));

        let mut out = RecvBatch::with_capacity(1);
        assert_eq!(socket.recv(&mut out).unwrap(), 1);
        assert_eq!(out.as_slice()[0].meta.version, IpVersion::V4);
        assert_eq!(out.as_slice()[0].packet.as_slice(), &ip);
    }

    #[test]
    fn xdp_ip_packet_socket_drops_ipv4_fragments() {
        let mut socket =
            XdpIpPacketSocketBuilder::new(IfIndex::new(1), QueueId::new(0)).open_busy_poll_test();
        let mut ip = ipv4_packet();
        ip[6] = 0x20;

        assert!(!socket.push_received_ethernet_frame(&ethernet_frame(&ip)));
        assert_eq!(socket.stats(QueueId::new(0)).dropped_fragments, 1);

        let mut out = RecvBatch::with_capacity(1);
        assert_eq!(socket.recv(&mut out).unwrap(), 0);
    }

    #[test]
    fn xdp_ip_packet_socket_rejects_trailing_tx_bytes() {
        let mut socket =
            XdpIpPacketSocketBuilder::new(IfIndex::new(1), QueueId::new(0)).open_busy_poll_test();
        let mut packet = socket.tx_pool_mut().allocate().unwrap();
        packet.extend_from_slice(&ipv4_packet()).unwrap();
        packet.extend_from_slice(b"extra").unwrap();
        let mut batch = [TxSlot::Ready(IpPacketTransmit::new(
            packet.freeze(),
            egress(),
        ))];

        let error = socket
            .send(&mut batch)
            .expect_err("trailing bytes are invalid");
        assert_eq!(error.accepted, 0);
        assert!(matches!(error.kind, Error::InvalidPacket));
        assert!(batch[0].is_ready());
    }

    #[test]
    fn xdp_udp_socket_sends_ipv4_udp_ethernet_frame() {
        let local = SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 10), 9000);
        let remote = SocketAddrV4::new(Ipv4Addr::new(198, 51, 100, 20), 9001);
        let mut socket = XdpUdpSocket::builder(IfIndex::new(1), QueueId::new(0), local)
            .route_snapshot(route_snapshot_for_gateway(
                IfIndex::new(1),
                QueueId::new(0),
                Ipv4Addr::new(192, 0, 2, 1),
                egress().dst_mac,
                egress().src_mac,
            ))
            .open_busy_poll_test();
        let mut packet = socket.tx_pool_mut().allocate().unwrap();
        packet.extend_from_slice(b"hello").unwrap();
        let mut batch = [TxSlot::Ready(UdpTransmit::new(
            packet.freeze(),
            remote.into(),
        ))];

        assert_eq!(socket.send(&mut batch).unwrap(), 1);
        assert!(batch[0].is_taken());
        assert_eq!(socket.ip.pending_tx_frame_count(), 1);
        assert_eq!(fast_socket_rs::RawDevice::ifindex(&socket), IfIndex::new(1));
        let stats = fast_socket_rs::RawDevice::stats(&socket, QueueId::new(0));
        assert_eq!(stats.tx_packets, 1);
        assert_eq!(stats.tx_bytes, 33);

        let frame = socket.ip.pending_tx_frame(0).unwrap();
        assert_eq!(&frame[..6], &[1, 2, 3, 4, 5, 6]);
        assert_eq!(&frame[6..12], &[6, 5, 4, 3, 2, 1]);
        assert_eq!(&frame[12..14], &ETHERTYPE_IPV4.to_be_bytes());
        let ip = &frame[14..];
        assert_eq!(ip[0], 0x45);
        assert_eq!(usize::from(u16::from_be_bytes([ip[2], ip[3]])), 33);
        assert_eq!(ip[8], 64);
        assert_eq!(ip[9], UDP_PROTOCOL);
        assert_eq!(&ip[12..16], &local.ip().octets());
        assert_eq!(&ip[16..20], &remote.ip().octets());
        assert_eq!(u16::from_be_bytes([ip[20], ip[21]]), local.port());
        assert_eq!(u16::from_be_bytes([ip[22], ip[23]]), remote.port());
        assert_eq!(usize::from(u16::from_be_bytes([ip[24], ip[25]])), 13);
        assert_eq!(&ip[28..], b"hello");
    }

    #[test]
    fn xdp_udp_socket_resolves_route_and_arp_from_local_cache_on_send() {
        let local = SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 10), 9000);
        let remote = SocketAddrV4::new(Ipv4Addr::new(198, 51, 100, 20), 9001);
        let gateway = Ipv4Addr::new(192, 0, 2, 1);
        let route_dst_mac = mac(0x2a);
        let route_src_mac = mac(0x3b);
        let mut socket = XdpUdpSocket::builder(IfIndex::new(1), QueueId::new(0), local)
            .route_snapshot(route_snapshot_for_gateway(
                IfIndex::new(1),
                QueueId::new(0),
                gateway,
                route_dst_mac,
                route_src_mac,
            ))
            .open_busy_poll_test();
        let mut packet = socket.tx_pool_mut().allocate().unwrap();
        packet.extend_from_slice(b"hello").unwrap();
        let mut batch = [TxSlot::Ready(UdpTransmit::new(
            packet.freeze(),
            remote.into(),
        ))];

        assert_eq!(socket.send(&mut batch).unwrap(), 1);

        let frame = socket.ip.pending_tx_frame(0).unwrap();
        assert_eq!(&frame[..6], &route_dst_mac.octets());
        assert_eq!(&frame[6..12], &route_src_mac.octets());
        let ip = &frame[ETHERNET_HEADER_LEN..];
        assert_eq!(&ip[16..20], &remote.ip().octets());
    }

    #[test]
    fn xdp_udp_socket_accepts_custom_router() {
        let local = SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 10), 9000);
        let remote = SocketAddrV4::new(Ipv4Addr::new(198, 51, 100, 20), 9001);
        let mut socket = XdpUdpSocket::builder(IfIndex::new(1), QueueId::new(0), local)
            .router(StaticUdpRouter { egress: egress() })
            .open_busy_poll_test();
        let mut packet = socket.tx_pool_mut().allocate().unwrap();
        packet.extend_from_slice(b"hello").unwrap();
        let mut batch = [TxSlot::Ready(UdpTransmit::new(
            packet.freeze(),
            remote.into(),
        ))];

        assert_eq!(socket.send(&mut batch).unwrap(), 1);

        let frame = socket.ip.pending_tx_frame(0).unwrap();
        assert_eq!(&frame[..6], &[1, 2, 3, 4, 5, 6]);
        assert_eq!(&frame[6..12], &[6, 5, 4, 3, 2, 1]);
    }

    #[test]
    fn routed_xdp_udp_socket_rejects_missing_route_without_consuming_slot() {
        let local = SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 10), 9000);
        let remote = SocketAddrV4::new(Ipv4Addr::new(198, 51, 100, 20), 9001);
        let mut socket =
            XdpUdpSocket::builder(IfIndex::new(1), QueueId::new(0), local).open_busy_poll_test();
        let mut packet = socket.tx_pool_mut().allocate().unwrap();
        packet.extend_from_slice(b"hello").unwrap();
        let mut batch = [TxSlot::Ready(UdpTransmit::new(
            packet.freeze(),
            remote.into(),
        ))];

        let error = socket.send(&mut batch).expect_err("route cache is empty");

        assert_eq!(error.accepted, 0);
        assert!(matches!(error.kind, Error::NoEgressRoute));
        assert!(batch[0].is_ready());
        assert_eq!(socket.ip.pending_tx_frame_count(), 0);
    }

    #[test]
    fn xdp_udp_socket_receives_udp_payload_from_ethernet_frame() {
        let local = SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 10), 9000);
        let remote = SocketAddrV4::new(Ipv4Addr::new(198, 51, 100, 20), 9001);
        let mut socket =
            XdpUdpSocket::builder(IfIndex::new(1), QueueId::new(0), local).open_busy_poll_test();
        let ip = ipv4_udp_packet(
            *remote.ip(),
            *local.ip(),
            remote.port(),
            local.port(),
            b"pong",
        );
        assert!(socket.ip.push_received_ethernet_frame(&ethernet_frame(&ip)));

        let mut out = RecvBatch::with_capacity(1);
        assert_eq!(socket.recv(&mut out).unwrap(), 1);
        let item = &out.as_slice()[0];
        assert_eq!(item.packet.as_slice(), b"pong");
        assert_eq!(item.meta.source, remote.into());
        assert_eq!(item.meta.destination, Some(IpAddr::V4(*local.ip())));
        assert_eq!(item.meta.len, 4);
        let stats = fast_socket_rs::RawDevice::stats(&socket, QueueId::new(0));
        assert_eq!(stats.rx_packets, 1);
        assert_eq!(stats.rx_bytes, 32);
    }

    #[test]
    fn xdp_udp_direct_parser_reads_ipv4_udp_from_ethernet_frame() {
        let local = SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 10), 9000);
        let remote = SocketAddrV4::new(Ipv4Addr::new(198, 51, 100, 20), 9001);
        let payload = b"pong";
        let ip = ipv4_udp_packet(
            *remote.ip(),
            *local.ip(),
            remote.port(),
            local.port(),
            payload,
        );
        let frame = ethernet_frame(&ip);

        let parsed = match parse_ethernet_ipv4_udp(&frame) {
            UdpParse::Udp(parsed) => parsed,
            other => panic!("expected UDP frame, got {other:?}"),
        };

        assert_eq!(parsed.source, *remote.ip());
        assert_eq!(parsed.destination, *local.ip());
        assert_eq!(parsed.source_port, remote.port());
        assert_eq!(parsed.destination_port, local.port());
        assert_eq!(
            parsed.ip_len,
            IPV4_MIN_HEADER_LEN + UDP_HEADER_LEN + payload.len()
        );
        assert_eq!(
            parsed.payload_offset,
            ETHERNET_HEADER_LEN + IPV4_MIN_HEADER_LEN + UDP_HEADER_LEN
        );
        assert_eq!(parsed.payload_len, payload.len());
        assert_eq!(
            &frame[parsed.payload_offset..parsed.payload_offset + parsed.payload_len],
            payload
        );
    }
}
