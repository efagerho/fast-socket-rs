//! Two-phase factory for AF_XDP aggregate sockets.
//!
//! Phase 1 ([`XdpFactoryBuilder`] -> [`XdpFactory`], any thread) discovers NIC
//! queues, attaches one eBPF program per interface, fills the port filter, and
//! partitions the claimed queues into `threads(T)` contiguous blocks — one
//! [`XdpWorkerPlan`] (one aggregate socket) per block.
//!
//! Phase 2 (per worker thread) moves one `Send` plan to a thread and calls
//! `plan.open_*`, which pins the thread to `plan.cpu()` and opens that worker's
//! aggregate socket — all member queues sharing one NUMA-local UMEM.
//!
//! The single `threads(T)` knob drives everything: `T` must divide the claimed
//! queue count `Q`, and each contiguous `Q/T` block must sit on a single
//! interface (shared-UMEM aggregates are single-interface) and NUMA node.

use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddrV4;

use fast_socket_rs::{
    BusyPollDriver, HugePageSize, IfIndex, NumaNode, QueueBufferConfig, QueueId,
    pin_current_thread_to_cpu,
};

use crate::aggregate::{XdpIpPacketAggregate, XdpUdpAggregate};
use crate::config::XdpIpPacketSocketConfig;
use crate::interface::{
    XdpQueueSlot, cpu_for_xdp_queue, if_index_to_name, if_name_to_index, numa_node_for_interface,
    xdp_queue_slots_for_interface,
};
use crate::program::{AttachMode, XdpProgramHandle};
use crate::raw_socket::{RingSizes, XdpMode};
use crate::route::RouteSnapshot;
use crate::socket::XdpQueueLocalRouter;

/// Selects the interface a factory binds to.
#[derive(Clone, Debug)]
pub enum InterfaceSelector {
    /// By interface name (e.g. `"eth0"`, `"bond0"`).
    Name(String),
    /// By operating-system interface index.
    Index(IfIndex),
}

/// Which discovered queue slots to bind sockets to.
#[derive(Clone, Debug)]
pub enum QueueClaim {
    /// Every discovered queue, in discovery order.
    All,
    /// The first `n` discovered queues.
    First(u32),
    /// An explicit set of flat queue indices (from
    /// [`xdp_queue_slots_for_interface`]), claimed in the given order.
    Queues(Vec<QueueId>),
}

/// Selects how the eBPF program filters traffic into the sockets.
#[derive(Clone, Debug)]
pub enum PortFilter {
    /// Redirect by the program's default (no UDP destination-port binding).
    AllIp,
    /// Redirect UDP traffic for this set of destination ports.
    UdpPorts(Vec<u16>),
}

/// Phase-1 factory builder. `Send`; build on any thread.
#[derive(Clone, Debug)]
pub struct XdpFactoryBuilder {
    iface: String,
    slots: Vec<XdpQueueSlot>,
    claim: QueueClaim,
    threads: Option<usize>,
    port_filter: PortFilter,
    frame_count: u32,
    huge_page_size: HugePageSize,
    mtu: usize,
    rings: RingSizes,
    mode: XdpMode,
    attach_mode: AttachMode,
    buffers: QueueBufferConfig,
    route_snapshot: RouteSnapshot,
}

impl XdpFactoryBuilder {
    /// Discovers the interface's XDP queue slots. Phase 1, any thread.
    pub fn new(iface: InterfaceSelector) -> io::Result<Self> {
        let iface = match iface {
            InterfaceSelector::Name(name) => name,
            InterfaceSelector::Index(index) => if_index_to_name(index)?,
        };
        let slots = xdp_queue_slots_for_interface(&iface)?;
        let defaults = XdpIpPacketSocketConfig::default();
        Ok(Self {
            iface,
            slots,
            claim: QueueClaim::All,
            threads: None,
            port_filter: PortFilter::AllIp,
            frame_count: defaults.frame_count,
            huge_page_size: defaults.huge_page_size,
            mtu: defaults.mtu,
            rings: defaults.rings,
            mode: defaults.mode,
            attach_mode: defaults.attach_mode,
            buffers: defaults.buffers,
            route_snapshot: RouteSnapshot::new(),
        })
    }

    /// Sets which queues to claim (default [`QueueClaim::All`]).
    #[must_use]
    pub fn claim(mut self, claim: QueueClaim) -> Self {
        self.claim = claim;
        self
    }

    /// Number of queues the current claim selects — read after `claim` to
    /// compute a `threads` value.
    #[must_use]
    pub fn claimed_queue_count(&self) -> u32 {
        match &self.claim {
            QueueClaim::All => self.slots.len() as u32,
            QueueClaim::First(n) => (*n as usize).min(self.slots.len()) as u32,
            QueueClaim::Queues(queues) => queues.len() as u32,
        }
    }

    /// Distinct IRQ CPUs across the claimed queues — e.g. pass to `threads` for
    /// one aggregate socket per IRQ CPU.
    pub fn irq_cpu_count(&self) -> io::Result<u32> {
        let mut cpus = std::collections::BTreeSet::new();
        for slot in self.claimed_slots_checked()? {
            cpus.insert(cpu_for_xdp_queue(&slot)?);
        }
        Ok(cpus.len() as u32)
    }

    /// Sets the worker-thread count `T`. `build()` checks `T` divides the
    /// claimed queue count. Default: one thread per claimed queue.
    #[must_use]
    pub fn threads(mut self, threads: usize) -> Self {
        self.threads = Some(threads);
        self
    }

    /// Sets the eBPF port filter (default [`PortFilter::AllIp`]).
    #[must_use]
    pub fn port_filter(mut self, filter: PortFilter) -> Self {
        self.port_filter = filter;
        self
    }

    /// Sets the **per-member** UMEM frame count.
    #[must_use]
    pub fn frame_count(mut self, frame_count: u32) -> Self {
        self.frame_count = frame_count;
        self
    }

    /// Sets the hugepage preference used for shared UMEM allocation.
    #[must_use]
    pub fn huge_page_size(mut self, huge_page_size: HugePageSize) -> Self {
        self.huge_page_size = huge_page_size;
        self
    }

    /// Sets the IP-layer MTU.
    #[must_use]
    pub fn mtu(mut self, mtu: usize) -> Self {
        self.mtu = mtu;
        self
    }

    /// Sets AF_XDP ring sizes.
    #[must_use]
    pub fn rings(mut self, rings: RingSizes) -> Self {
        self.rings = rings;
        self
    }

    /// Sets the AF_XDP bind mode.
    #[must_use]
    pub fn xdp_mode(mut self, mode: XdpMode) -> Self {
        self.mode = mode;
        self
    }

    /// Sets the XDP attach mode.
    #[must_use]
    pub fn attach_mode(mut self, attach_mode: AttachMode) -> Self {
        self.attach_mode = attach_mode;
        self
    }

    /// Sets per-queue buffer configuration.
    #[must_use]
    pub fn buffers(mut self, buffers: QueueBufferConfig) -> Self {
        self.buffers = buffers;
        self
    }

    /// Seeds the route/neighbor/link snapshot shared by opened sockets.
    #[must_use]
    pub fn route_snapshot(mut self, snapshot: RouteSnapshot) -> Self {
        self.route_snapshot = snapshot;
        self
    }

    fn claimed_slots(&self) -> Vec<XdpQueueSlot> {
        match &self.claim {
            QueueClaim::All => self.slots.clone(),
            QueueClaim::First(n) => self.slots.iter().take(*n as usize).cloned().collect(),
            QueueClaim::Queues(queues) => queues
                .iter()
                .filter_map(|flat| {
                    self.slots
                        .iter()
                        .find(|slot| slot.flat_index == *flat)
                        .cloned()
                })
                .collect(),
        }
    }

    fn claimed_slots_checked(&self) -> io::Result<Vec<XdpQueueSlot>> {
        let QueueClaim::Queues(queues) = &self.claim else {
            return Ok(self.claimed_slots());
        };

        let mut claimed = Vec::with_capacity(queues.len());
        let mut missing = Vec::new();
        for &flat in queues {
            if let Some(slot) = self.slots.iter().find(|slot| slot.flat_index == flat) {
                claimed.push(slot.clone());
            } else {
                missing.push(flat);
            }
        }

        if missing.is_empty() {
            return Ok(claimed);
        }

        let missing = missing
            .iter()
            .map(|queue| queue.get().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let available = self
            .slots
            .iter()
            .map(|slot| slot.flat_index.get().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "claimed XDP queue(s) [{missing}] are not available on {}; available flat queue indices: [{available}]",
                self.iface
            ),
        ))
    }

    /// Phase-1 build: attach programs, fill the filter, partition into worker
    /// plans. Validates `threads` divides the claim and that each block is on a
    /// single interface and NUMA node.
    pub fn build(self) -> io::Result<XdpFactory> {
        let claimed = self.claimed_slots_checked()?;
        if claimed.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("no XDP queues claimed on {}", self.iface),
            ));
        }
        let queue_count = claimed.len();
        let threads = self.threads.unwrap_or(queue_count);
        if threads == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "threads(T) must be >= 1",
            ));
        }
        if !queue_count.is_multiple_of(threads) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("threads({threads}) must divide the claimed queue count {queue_count}"),
            ));
        }
        let per_block = queue_count / threads;

        // Attach one program per distinct interface index among the claimed
        // queues (bond masters fan out to slave ifindexes).
        let mut programs: BTreeMap<IfIndex, XdpProgramHandle> = BTreeMap::new();
        for slot in &claimed {
            if let std::collections::btree_map::Entry::Vacant(entry) = programs.entry(slot.ifindex)
            {
                entry.insert(XdpProgramHandle::load(
                    slot.ifindex.get(),
                    self.attach_mode,
                    None,
                )?);
            }
        }

        // Fill the port filter once per program (bind_port is refcounted).
        if let PortFilter::UdpPorts(ports) = &self.port_filter {
            for program in programs.values() {
                let mut guard = program.lock().expect("XDP program mutex poisoned");
                for &port in ports {
                    guard.bind_port(port)?;
                }
            }
        }

        let mut plans = Vec::with_capacity(threads);
        for block in claimed.chunks(per_block) {
            // Single-interface invariant: one shared UMEM binds to one netdev.
            let ifindex = block[0].ifindex;
            if block.iter().any(|slot| slot.ifindex != ifindex) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "threads({threads}) produced a block spanning multiple interfaces on \
                         {}; choose a thread count whose blocks stay on one slave, or run one \
                         factory per slave",
                        self.iface
                    ),
                ));
            }
            let block_name = if_index_to_name(ifindex)?;
            let numa = numa_node_for_interface(&block_name).ok();
            let mut cpu = None;
            for slot in block {
                let slot_cpu = cpu_for_xdp_queue(slot)?;
                cpu = Some(cpu.map_or(slot_cpu, |best: u32| best.min(slot_cpu)));
            }
            let queues: Vec<QueueId> = block.iter().map(|slot| slot.queue).collect();

            let config = XdpIpPacketSocketConfig {
                ifindex,
                queue_id: queues[0],
                numa_node: numa,
                buffers: self.buffers,
                rings: self.rings,
                mode: self.mode,
                mtu: self.mtu,
                frame_count: self.frame_count,
                huge_page_size: self.huge_page_size,
                attach_mode: self.attach_mode,
                program_bytes: None,
                attached_program: Some(programs[&ifindex].clone()),
                // Filter ports were bound once above; the UDP opener binds the
                // local port itself (refcounted).
                bind_udp_port: None,
                route_snapshot: self.route_snapshot.clone(),
            };

            plans.push(XdpWorkerPlan {
                cpu: cpu.expect("non-empty block has a CPU"),
                numa,
                ifindex,
                queues,
                config,
            });
        }

        Ok(XdpFactory { plans, programs })
    }
}

/// Phase-1 result: per-worker plans plus the retained program handles.
pub struct XdpFactory {
    plans: Vec<XdpWorkerPlan>,
    programs: BTreeMap<IfIndex, XdpProgramHandle>,
}

impl XdpFactory {
    /// Borrows a loaded program handle by interface index.
    #[must_use]
    pub fn program_handle(&self, ifindex: IfIndex) -> Option<&XdpProgramHandle> {
        self.programs.get(&ifindex)
    }

    /// Consumes the factory into its `Send` per-worker plans. Each plan carries
    /// a cloned program handle that keeps the program attached.
    #[must_use]
    pub fn into_worker_plans(self) -> Vec<XdpWorkerPlan> {
        self.plans
    }
}

/// One worker thread's assignment: exactly one aggregate socket over the
/// thread's contiguous queue block, on one interface and NUMA node. `Send`.
#[derive(Clone, Debug)]
pub struct XdpWorkerPlan {
    cpu: u32,
    numa: Option<NumaNode>,
    ifindex: IfIndex,
    queues: Vec<QueueId>,
    config: XdpIpPacketSocketConfig,
}

impl XdpWorkerPlan {
    /// Lowest-numbered IRQ CPU among the block's queues — the core a worker
    /// owning this aggregate should pin to.
    #[must_use]
    pub fn cpu(&self) -> u32 {
        self.cpu
    }

    /// The single NUMA node the block's queues share, when known.
    #[must_use]
    pub fn numa_node(&self) -> Option<NumaNode> {
        self.numa
    }

    /// Interface index this worker's queues live on.
    #[must_use]
    pub fn ifindex(&self) -> IfIndex {
        self.ifindex
    }

    /// The NIC queues (on [`Self::ifindex`]) this worker drives.
    #[must_use]
    pub fn queue_ids(&self) -> &[QueueId] {
        &self.queues
    }

    /// Opens this worker's UDP aggregate, pinning the current thread to
    /// [`Self::cpu`] first so the UMEM, rings, and scratch are NUMA-local.
    pub fn open_udp_busy_poll(
        self,
        local: SocketAddrV4,
    ) -> io::Result<XdpUdpAggregate<BusyPollDriver, XdpQueueLocalRouter>> {
        pin_current_thread_to_cpu(self.cpu)?;
        self.open_udp_busy_poll_unpinned(local)
    }

    /// Opens this worker's UDP aggregate **without** pinning (caller must pin to
    /// [`Self::cpu`] first to stay NUMA-local).
    pub fn open_udp_busy_poll_unpinned(
        self,
        local: SocketAddrV4,
    ) -> io::Result<XdpUdpAggregate<BusyPollDriver, XdpQueueLocalRouter>> {
        let mut config = self.config;
        config.bind_udp_port = Some(local.port());
        XdpUdpAggregate::open_busy_poll(config, &self.queues, local)
    }

    /// Opens this worker's UDP aggregate with a caller-supplied
    /// [`XdpUdpRouter`](crate::XdpUdpRouter) built per member by `make_router`,
    /// pinning to [`Self::cpu`] first. Use when the default queue-local router
    /// is not wanted.
    pub fn open_udp_busy_poll_with_router<R>(
        self,
        local: SocketAddrV4,
        make_router: impl FnMut() -> R,
    ) -> io::Result<XdpUdpAggregate<BusyPollDriver, R>> {
        pin_current_thread_to_cpu(self.cpu)?;
        let mut config = self.config;
        config.bind_udp_port = Some(local.port());
        XdpUdpAggregate::open_busy_poll_with(config, &self.queues, local, make_router)
    }

    /// Opens this worker's IP-packet aggregate, pinning the current thread to
    /// [`Self::cpu`] first.
    pub fn open_ip_packet_busy_poll(self) -> io::Result<XdpIpPacketAggregate<BusyPollDriver>> {
        pin_current_thread_to_cpu(self.cpu)?;
        self.open_ip_packet_busy_poll_unpinned()
    }

    /// Opens this worker's IP-packet aggregate **without** pinning.
    pub fn open_ip_packet_busy_poll_unpinned(
        self,
    ) -> io::Result<XdpIpPacketAggregate<BusyPollDriver>> {
        XdpIpPacketAggregate::open_busy_poll(self.config, &self.queues)
    }
}

/// Resolves an interface selector to an index (helper for callers).
pub fn resolve_interface_index(selector: &InterfaceSelector) -> io::Result<IfIndex> {
    match selector {
        InterfaceSelector::Name(name) => if_name_to_index(name),
        InterfaceSelector::Index(index) => Ok(*index),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slot(flat_index: u32) -> XdpQueueSlot {
        XdpQueueSlot::new(
            "eth-test".to_string(),
            IfIndex::new(10),
            QueueId::new(flat_index),
            QueueId::new(flat_index),
        )
    }

    fn builder_with_slots(slots: Vec<XdpQueueSlot>) -> XdpFactoryBuilder {
        let defaults = XdpIpPacketSocketConfig::default();
        XdpFactoryBuilder {
            iface: "eth-test".to_string(),
            slots,
            claim: QueueClaim::All,
            threads: None,
            port_filter: PortFilter::AllIp,
            frame_count: defaults.frame_count,
            huge_page_size: defaults.huge_page_size,
            mtu: defaults.mtu,
            rings: defaults.rings,
            mode: defaults.mode,
            attach_mode: defaults.attach_mode,
            buffers: defaults.buffers,
            route_snapshot: RouteSnapshot::new(),
        }
    }

    #[test]
    fn explicit_queue_claim_preserves_requested_order() {
        let builder = builder_with_slots(vec![slot(0), slot(1)])
            .claim(QueueClaim::Queues(vec![QueueId::new(1), QueueId::new(0)]));

        let claimed = builder.claimed_slots_checked().unwrap();

        assert_eq!(
            claimed
                .iter()
                .map(|slot| slot.flat_index.get())
                .collect::<Vec<_>>(),
            vec![1, 0]
        );
    }

    #[test]
    fn explicit_queue_claim_rejects_missing_flat_indices() {
        let builder = builder_with_slots(vec![slot(0), slot(1)])
            .claim(QueueClaim::Queues(vec![QueueId::new(0), QueueId::new(3)]));

        let error = builder.claimed_slots_checked().unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("[3]"));
        assert!(error.to_string().contains("[0, 1]"));
    }

    #[test]
    fn explicit_queue_count_reports_requested_count() {
        let builder = builder_with_slots(vec![slot(0)])
            .claim(QueueClaim::Queues(vec![QueueId::new(0), QueueId::new(3)]));

        assert_eq!(builder.claimed_queue_count(), 2);
    }

    #[test]
    fn builder_records_huge_page_preference() {
        let builder = builder_with_slots(vec![slot(0)]).huge_page_size(HugePageSize::Size4K);

        assert_eq!(builder.huge_page_size, HugePageSize::Size4K);
    }
}
