//! Aggregate AF_XDP sockets: one logical socket fed by 1..N NIC queues.
//!
//! An aggregate owns one single-queue [`XdpUdpSocket`]/[`XdpIpPacketSocket`] per
//! claimed NIC queue and multiplexes `recv`/`send` across them, so a worker
//! thread drives several queues through one object. The single-queue sockets
//! are the proven N=1 building blocks; the aggregate adds round-robin fan-in on
//! receive and round-robin spread on transmit.
//!
//! Members opened by the aggregate constructors share one UMEM (allocated and
//! registered once) while each member owns separate RX/TX/FILL/COMPLETION
//! rings. Frame addresses are UMEM-relative, so a buffer from one member can be
//! submitted through another member's TX ring as long as no frame is in flight
//! on more than one ring at a time; the completing member reclaims the frame
//! into its local RX or TX pool according to the shared UMEM frame layout.

use std::io;
use std::net::SocketAddrV4;

use fast_socket_rs::{
    BusyPollDriver, Error, IpPacketRxItem, IpPacketSocket, PollDriver, QueueId, RecvBatch,
    UdpReceive, UdpRxBuffer, UdpSocket,
};

use crate::config::XdpIpPacketSocketConfig;
use crate::socket::{
    XdpIpPacketSocket, XdpQueueLocalRouter, XdpUdpAcceptedPorts, XdpUdpRouter, XdpUdpSocket,
    XdpWaitDrivenDriver,
};

/// One logical AF_XDP IP-packet socket fed by 1..N NIC queues.
pub struct XdpIpPacketAggregate<D> {
    members: Vec<XdpIpPacketSocket<D>>,
    rx_rr: usize,
    shared_umem: bool,
}

impl<D> XdpIpPacketAggregate<D> {
    /// Builds an aggregate from one or more opened single-queue sockets.
    ///
    /// Returns an error if `members` is empty.
    pub fn from_members(members: Vec<XdpIpPacketSocket<D>>) -> io::Result<Self> {
        if members.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "XdpIpPacketAggregate requires at least one member socket",
            ));
        }
        Ok(Self {
            members,
            rx_rr: 0,
            shared_umem: false,
        })
    }

    fn from_shared_umem_members(members: Vec<XdpIpPacketSocket<D>>) -> io::Result<Self> {
        Self::from_members(members).map(|mut aggregate| {
            aggregate.shared_umem = true;
            aggregate
        })
    }

    /// Number of NIC queues (member sockets) backing this aggregate.
    #[must_use]
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// Returns `true` if the aggregate has no members (never, post-construction).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Member sockets, for per-queue processing (e.g. reflect/forward where each
    /// frame must leave on the queue it arrived on).
    #[must_use]
    pub fn members_mut(&mut self) -> &mut [XdpIpPacketSocket<D>] {
        &mut self.members
    }

    /// Consumes the aggregate and returns its member sockets.
    #[must_use]
    pub fn into_members(self) -> Vec<XdpIpPacketSocket<D>> {
        self.members
    }

    /// Returns whether this aggregate's members were opened over one shared UMEM.
    #[must_use]
    pub const fn members_share_umem(&self) -> bool {
        self.shared_umem
    }
}

impl XdpIpPacketAggregate<BusyPollDriver> {
    /// Opens one busy-poll aggregate over `queues`, all sharing a single UMEM.
    ///
    /// `config.frame_count` is the per-member frame count. See
    /// [`XdpIpPacketSocket::open_shared_busy_poll`].
    pub fn open_busy_poll(config: XdpIpPacketSocketConfig, queues: &[QueueId]) -> io::Result<Self> {
        Self::from_shared_umem_members(XdpIpPacketSocket::open_shared_busy_poll(config, queues)?)
    }
}

impl XdpIpPacketAggregate<XdpWaitDrivenDriver> {
    /// Opens one wait-driven aggregate over `queues`, all sharing a single
    /// UMEM.
    ///
    /// `config.frame_count` is the per-member frame count. See
    /// [`XdpIpPacketSocket::open_shared_wait_driven`].
    pub fn open_wait_driven(
        config: XdpIpPacketSocketConfig,
        queues: &[QueueId],
    ) -> io::Result<Self> {
        Self::from_shared_umem_members(XdpIpPacketSocket::open_shared_wait_driven(config, queues)?)
    }
}

impl<D: PollDriver> XdpIpPacketAggregate<D> {
    /// Receives across all members in one round-robin sweep, appending into
    /// `out` until it is full or every member's RX ring has been visited.
    ///
    /// Advances the round-robin cursor so no single queue can starve the
    /// others across calls.
    pub fn recv(
        &mut self,
        out: &mut RecvBatch<IpPacketRxItem<XdpIpPacketSocket<D>>>,
    ) -> Result<usize, Error> {
        let n = self.members.len();
        let mut total = 0usize;
        let mut cursor = self.rx_rr;
        for _ in 0..n {
            if out.remaining() == 0 {
                break;
            }
            total += self.members[cursor % n].recv(out)?;
            cursor += 1;
        }
        self.rx_rr = cursor % n;
        Ok(total)
    }

    /// Drains transmit completions on every member, summing the counts.
    pub fn drain_tx_completions(&mut self) -> Result<usize, Error> {
        let mut total = 0usize;
        for member in &mut self.members {
            total += member.drain_tx_completions()?;
        }
        Ok(total)
    }
}

/// One logical AF_XDP UDP socket fed by 1..N NIC queues.
pub struct XdpUdpAggregate<D, R> {
    members: Vec<XdpUdpSocket<D, R>>,
    rx_rr: usize,
    tx_rr: usize,
    shared_umem: bool,
}

impl<D, R> XdpUdpAggregate<D, R> {
    /// Builds an aggregate from one or more opened single-queue sockets.
    ///
    /// Returns an error if `members` is empty.
    pub fn from_members(members: Vec<XdpUdpSocket<D, R>>) -> io::Result<Self> {
        if members.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "XdpUdpAggregate requires at least one member socket",
            ));
        }
        Ok(Self {
            members,
            rx_rr: 0,
            tx_rr: 0,
            shared_umem: false,
        })
    }

    fn from_shared_umem_members(members: Vec<XdpUdpSocket<D, R>>) -> io::Result<Self> {
        Self::from_members(members).map(|mut aggregate| {
            aggregate.shared_umem = true;
            aggregate
        })
    }

    /// Number of NIC queues (member sockets) backing this aggregate.
    #[must_use]
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// Returns `true` if the aggregate has no members (never, post-construction).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// All member sockets.
    #[must_use]
    pub fn members_mut(&mut self) -> &mut [XdpUdpSocket<D, R>] {
        &mut self.members
    }

    /// Consumes the aggregate and returns its member sockets.
    #[must_use]
    pub fn into_members(self) -> Vec<XdpUdpSocket<D, R>> {
        self.members
    }

    /// Returns whether this aggregate's members were opened over one shared UMEM.
    #[must_use]
    pub const fn members_share_umem(&self) -> bool {
        self.shared_umem
    }

    /// Returns the next member in round-robin order for transmit.
    ///
    /// Back-to-back calls rotate across queues, spreading TX so no single NIC TX
    /// ring is hot. Aggregates opened by this crate's shared-UMEM constructors
    /// may submit any member's UMEM-backed buffer through the returned member;
    /// aggregates assembled with [`Self::from_members`] should allocate and send
    /// on the same member unless the caller knows those members share a UMEM.
    #[must_use]
    pub fn next_tx_member(&mut self) -> &mut XdpUdpSocket<D, R> {
        let index = self.tx_rr % self.members.len();
        self.tx_rr = self.tx_rr.wrapping_add(1);
        &mut self.members[index]
    }
}

impl XdpUdpAggregate<BusyPollDriver, XdpQueueLocalRouter> {
    /// Opens one busy-poll UDP aggregate over `queues`, all sharing a single
    /// UMEM. `config.frame_count` is the per-member frame count. See
    /// [`XdpUdpSocket::open_shared_busy_poll`].
    pub fn open_busy_poll(
        config: XdpIpPacketSocketConfig,
        queues: &[QueueId],
        local_addr: SocketAddrV4,
    ) -> io::Result<Self> {
        Self::from_shared_umem_members(XdpUdpSocket::open_shared_busy_poll(
            config, queues, local_addr,
        )?)
    }

    /// Opens one busy-poll UDP aggregate accepting the given UDP destination
    /// ports in the userspace UDP parser.
    pub fn open_busy_poll_accepting_ports(
        config: XdpIpPacketSocketConfig,
        queues: &[QueueId],
        local_addr: SocketAddrV4,
        ports: impl IntoIterator<Item = u16>,
    ) -> io::Result<Self> {
        Self::from_shared_umem_members(XdpUdpSocket::open_shared_busy_poll_accepting(
            config,
            queues,
            local_addr,
            XdpUdpAcceptedPorts::ports(ports.into_iter().collect()),
        )?)
    }

    /// Opens one busy-poll UDP aggregate accepting an inclusive UDP destination
    /// port range in the userspace UDP parser.
    pub fn open_busy_poll_accepting_port_range(
        config: XdpIpPacketSocketConfig,
        queues: &[QueueId],
        local_addr: SocketAddrV4,
        start: u16,
        end: u16,
    ) -> io::Result<Self> {
        Self::from_shared_umem_members(XdpUdpSocket::open_shared_busy_poll_accepting(
            config,
            queues,
            local_addr,
            XdpUdpAcceptedPorts::range(start, end),
        )?)
    }
}

impl XdpUdpAggregate<XdpWaitDrivenDriver, XdpQueueLocalRouter> {
    /// Opens one wait-driven UDP aggregate over `queues`, all sharing a single
    /// UMEM. `config.frame_count` is the per-member frame count. See
    /// [`XdpUdpSocket::open_shared_wait_driven`].
    pub fn open_wait_driven(
        config: XdpIpPacketSocketConfig,
        queues: &[QueueId],
        local_addr: SocketAddrV4,
    ) -> io::Result<Self> {
        Self::from_shared_umem_members(XdpUdpSocket::open_shared_wait_driven(
            config, queues, local_addr,
        )?)
    }

    /// Opens one wait-driven UDP aggregate accepting the given UDP destination
    /// ports in the userspace UDP parser.
    pub fn open_wait_driven_accepting_ports(
        config: XdpIpPacketSocketConfig,
        queues: &[QueueId],
        local_addr: SocketAddrV4,
        ports: impl IntoIterator<Item = u16>,
    ) -> io::Result<Self> {
        Self::from_shared_umem_members(XdpUdpSocket::open_shared_wait_driven_accepting(
            config,
            queues,
            local_addr,
            XdpUdpAcceptedPorts::ports(ports.into_iter().collect()),
        )?)
    }

    /// Opens one wait-driven UDP aggregate accepting an inclusive UDP
    /// destination port range in the userspace UDP parser.
    pub fn open_wait_driven_accepting_port_range(
        config: XdpIpPacketSocketConfig,
        queues: &[QueueId],
        local_addr: SocketAddrV4,
        start: u16,
        end: u16,
    ) -> io::Result<Self> {
        Self::from_shared_umem_members(XdpUdpSocket::open_shared_wait_driven_accepting(
            config,
            queues,
            local_addr,
            XdpUdpAcceptedPorts::range(start, end),
        )?)
    }
}

impl<R> XdpUdpAggregate<BusyPollDriver, R> {
    /// Opens one busy-poll UDP aggregate over `queues` (single shared UMEM)
    /// with a caller-supplied [`XdpUdpRouter`] built per member by
    /// `make_router`.
    pub fn open_busy_poll_with(
        config: XdpIpPacketSocketConfig,
        queues: &[QueueId],
        local_addr: SocketAddrV4,
        make_router: impl FnMut() -> R,
    ) -> io::Result<Self> {
        Self::from_shared_umem_members(XdpUdpSocket::open_shared_busy_poll_with(
            config,
            queues,
            local_addr,
            make_router,
        )?)
    }

    /// Opens one busy-poll UDP aggregate with a custom router while accepting
    /// the given UDP destination ports in the userspace UDP parser.
    pub fn open_busy_poll_with_accepting_ports(
        config: XdpIpPacketSocketConfig,
        queues: &[QueueId],
        local_addr: SocketAddrV4,
        ports: impl IntoIterator<Item = u16>,
        make_router: impl FnMut() -> R,
    ) -> io::Result<Self> {
        Self::from_shared_umem_members(XdpUdpSocket::open_shared_busy_poll_with_accepting(
            config,
            queues,
            local_addr,
            XdpUdpAcceptedPorts::ports(ports.into_iter().collect()),
            make_router,
        )?)
    }

    /// Opens one busy-poll UDP aggregate with a custom router while accepting an
    /// inclusive UDP destination port range in the userspace UDP parser.
    pub fn open_busy_poll_with_accepting_port_range(
        config: XdpIpPacketSocketConfig,
        queues: &[QueueId],
        local_addr: SocketAddrV4,
        start: u16,
        end: u16,
        make_router: impl FnMut() -> R,
    ) -> io::Result<Self> {
        Self::from_shared_umem_members(XdpUdpSocket::open_shared_busy_poll_with_accepting(
            config,
            queues,
            local_addr,
            XdpUdpAcceptedPorts::range(start, end),
            make_router,
        )?)
    }
}

impl<R> XdpUdpAggregate<XdpWaitDrivenDriver, R> {
    /// Opens one wait-driven UDP aggregate over `queues` (single shared UMEM)
    /// with a caller-supplied [`XdpUdpRouter`](crate::XdpUdpRouter) built per
    /// member by `make_router`.
    pub fn open_wait_driven_with(
        config: XdpIpPacketSocketConfig,
        queues: &[QueueId],
        local_addr: SocketAddrV4,
        make_router: impl FnMut() -> R,
    ) -> io::Result<Self> {
        Self::from_shared_umem_members(XdpUdpSocket::open_shared_wait_driven_with(
            config,
            queues,
            local_addr,
            make_router,
        )?)
    }

    /// Opens one wait-driven UDP aggregate with a custom router while accepting
    /// the given UDP destination ports in the userspace UDP parser.
    pub fn open_wait_driven_with_accepting_ports(
        config: XdpIpPacketSocketConfig,
        queues: &[QueueId],
        local_addr: SocketAddrV4,
        ports: impl IntoIterator<Item = u16>,
        make_router: impl FnMut() -> R,
    ) -> io::Result<Self> {
        Self::from_shared_umem_members(XdpUdpSocket::open_shared_wait_driven_with_accepting(
            config,
            queues,
            local_addr,
            XdpUdpAcceptedPorts::ports(ports.into_iter().collect()),
            make_router,
        )?)
    }

    /// Opens one wait-driven UDP aggregate with a custom router while accepting
    /// an inclusive UDP destination port range in the userspace UDP parser.
    pub fn open_wait_driven_with_accepting_port_range(
        config: XdpIpPacketSocketConfig,
        queues: &[QueueId],
        local_addr: SocketAddrV4,
        start: u16,
        end: u16,
        make_router: impl FnMut() -> R,
    ) -> io::Result<Self> {
        Self::from_shared_umem_members(XdpUdpSocket::open_shared_wait_driven_with_accepting(
            config,
            queues,
            local_addr,
            XdpUdpAcceptedPorts::range(start, end),
            make_router,
        )?)
    }
}

impl<D, R> XdpUdpAggregate<D, R>
where
    D: PollDriver,
    R: XdpUdpRouter,
{
    /// Receives across all members in one round-robin sweep (see
    /// [`XdpIpPacketAggregate::recv`]).
    pub fn recv(&mut self, out: &mut RecvBatch<UdpRecvItemOf<D, R>>) -> Result<usize, Error> {
        let n = self.members.len();
        let mut total = 0usize;
        let mut cursor = self.rx_rr;
        for _ in 0..n {
            if out.remaining() == 0 {
                break;
            }
            total += self.members[cursor % n].recv(out)?;
            cursor += 1;
        }
        self.rx_rr = cursor % n;
        Ok(total)
    }

    /// Drains transmit completions on every member, summing the counts.
    pub fn drain_tx_completions(&mut self) -> Result<usize, Error> {
        let mut total = 0usize;
        for member in &mut self.members {
            total += member.drain_tx_completions()?;
        }
        Ok(total)
    }
}

/// The receive-metadata type of an `XdpUdpSocket<D, R>`.
type UdpRecvMetaOf<D, R> = <XdpUdpSocket<D, R> as UdpSocket>::RecvMeta;

/// The receive-batch item type of an `XdpUdpSocket<D, R>`.
type UdpRecvItemOf<D, R> = UdpReceive<UdpRxBuffer<XdpUdpSocket<D, R>>, UdpRecvMetaOf<D, R>>;
