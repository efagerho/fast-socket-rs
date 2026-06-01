//! AF_XDP egress handle types.

use fast_socket_rs::{IfIndex, IpPacketEgress, LinkAddr, QueueId};

/// Ethernet ethertype for IPv4.
pub const ETHERTYPE_IPV4: u16 = 0x0800;

/// Ethernet ethertype for IPv6.
pub const ETHERTYPE_IPV6: u16 = 0x86dd;

pub(crate) const ETHERNET_HEADER_LEN: usize = 14;
pub(crate) const VLAN_HEADER_LEN: usize = 18;
pub(crate) const VLAN_ETHERTYPE: u16 = 0x8100;

/// Fully resolved egress data consumed by AF_XDP transmit.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct XdpEgress {
    /// Outgoing interface index.
    pub ifindex: IfIndex,
    /// Outgoing queue id.
    pub queue: QueueId,
    /// Destination link-layer address.
    pub dst_mac: LinkAddr,
    /// Source link-layer address.
    pub src_mac: LinkAddr,
    /// Ethernet payload type.
    pub ethertype: u16,
    /// Optional VLAN identifier.
    pub vlan: Option<u16>,
    /// Effective IP-layer MTU for this egress.
    pub mtu: u32,
}

impl XdpEgress {
    /// Creates an IPv4 Ethernet egress handle.
    #[must_use]
    pub const fn ipv4(
        ifindex: IfIndex,
        queue: QueueId,
        dst_mac: LinkAddr,
        src_mac: LinkAddr,
        mtu: u32,
    ) -> Self {
        Self {
            ifindex,
            queue,
            dst_mac,
            src_mac,
            ethertype: ETHERTYPE_IPV4,
            vlan: None,
            mtu,
        }
    }

    /// Returns this egress with the given 802.1Q VLAN id attached.
    ///
    /// Prefer this over `XdpEgress { vlan: Some(...), ..base }` field-init
    /// syntax: the builder version is self-documenting at call sites and
    /// keeps the struct's field surface internal to the crate.
    #[must_use]
    pub const fn with_vlan(mut self, vlan: u16) -> Self {
        self.vlan = Some(vlan);
        self
    }

    /// Returns this egress with any previously-attached VLAN cleared.
    #[must_use]
    pub const fn without_vlan(mut self) -> Self {
        self.vlan = None;
        self
    }
}

impl IpPacketEgress for XdpEgress {}

/// AF_XDP egress with materialized L2 header bytes.
///
/// UDP routers can return this when route, neighbor, and interface facts are
/// stable enough to build the Ethernet header outside the send hot path.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct XdpResolvedEgress {
    egress: XdpEgress,
    l2_header: [u8; VLAN_HEADER_LEN],
    l2_len: usize,
}

impl XdpResolvedEgress {
    /// Builds a resolved egress by materializing the Ethernet or VLAN header.
    #[must_use]
    pub fn from_egress(egress: XdpEgress) -> Self {
        let (l2_header, l2_len) = build_ethernet_header(egress);
        Self {
            egress,
            l2_header,
            l2_len,
        }
    }

    /// Returns the underlying egress handle.
    #[must_use]
    pub const fn egress(&self) -> XdpEgress {
        self.egress
    }

    /// Returns the materialized Ethernet header bytes.
    #[must_use]
    pub fn l2_header(&self) -> &[u8] {
        &self.l2_header[..self.l2_len]
    }

    pub(crate) const fn l2_header_array(&self) -> [u8; VLAN_HEADER_LEN] {
        self.l2_header
    }

    pub(crate) const fn l2_len(&self) -> usize {
        self.l2_len
    }

    pub(crate) const fn mtu(&self) -> u32 {
        self.egress.mtu
    }

    pub(crate) fn with_queue(mut self, queue: QueueId) -> Self {
        self.egress.queue = queue;
        self
    }
}

/// A resolved Ethernet header for one outgoing UDP datagram, plus the effective
/// IP-layer MTU.
///
/// Returned on the transmit hot path by
/// [`XdpUdpRouter::resolve_udp_l2`](crate::socket::XdpUdpRouter::resolve_udp_l2).
/// Gateway routes borrow a prebuilt header (no per-packet copy); on-link
/// destinations, whose next-hop MAC varies per packet, carry a header built
/// inline.
#[derive(Clone, Copy, Debug)]
pub enum ResolvedL2<'a> {
    /// Header bytes borrowed from a prebuilt (gateway) egress.
    Borrowed {
        /// Ethernet header bytes to prepend before the IP datagram.
        l2_header: &'a [u8],
        /// Effective IP-layer MTU for this datagram.
        ip_mtu: usize,
    },
    /// Header built inline for an on-link destination.
    Inline {
        /// Ethernet header buffer; only the first `l2_len` bytes are valid.
        l2_header: [u8; VLAN_HEADER_LEN],
        /// Valid length of `l2_header`.
        l2_len: usize,
        /// Effective IP-layer MTU for this datagram.
        ip_mtu: usize,
    },
}

impl ResolvedL2<'_> {
    /// Returns the Ethernet header bytes to prepend before the IP datagram.
    #[must_use]
    pub fn l2_header(&self) -> &[u8] {
        match self {
            Self::Borrowed { l2_header, .. } => l2_header,
            Self::Inline {
                l2_header, l2_len, ..
            } => &l2_header[..*l2_len],
        }
    }

    /// Returns the effective IP-layer MTU for this datagram.
    #[must_use]
    pub fn ip_mtu(&self) -> usize {
        match self {
            Self::Borrowed { ip_mtu, .. } | Self::Inline { ip_mtu, .. } => *ip_mtu,
        }
    }
}

pub(crate) fn ethernet_header_len(egress: XdpEgress) -> usize {
    if egress.vlan.is_some() {
        VLAN_HEADER_LEN
    } else {
        ETHERNET_HEADER_LEN
    }
}

/// Materializes the L2 header bytes for a resolved egress into a small buffer.
pub(crate) fn build_ethernet_header(egress: XdpEgress) -> ([u8; VLAN_HEADER_LEN], usize) {
    let l2_len = ethernet_header_len(egress);
    let mut header = [0u8; VLAN_HEADER_LEN];
    write_ethernet_header(&mut header[..l2_len], egress);
    (header, l2_len)
}

pub(crate) fn write_ethernet_header(header: &mut [u8], egress: XdpEgress) {
    debug_assert_eq!(header.len(), ethernet_header_len(egress));
    let dst_mac = egress.dst_mac.octets();
    let src_mac = egress.src_mac.octets();
    header[0..6].copy_from_slice(&dst_mac);
    header[6..12].copy_from_slice(&src_mac);
    if let Some(vlan) = egress.vlan {
        header[12..14].copy_from_slice(&VLAN_ETHERTYPE.to_be_bytes());
        header[14..16].copy_from_slice(&vlan.to_be_bytes());
        header[16..18].copy_from_slice(&egress.ethertype.to_be_bytes());
    } else {
        header[12..14].copy_from_slice(&egress.ethertype.to_be_bytes());
    }
}
