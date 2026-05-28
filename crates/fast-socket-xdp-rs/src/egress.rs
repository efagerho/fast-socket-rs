//! AF_XDP egress handle types.

use fast_socket_rs::{IfIndex, IpPacketEgress, LinkAddr, QueueId};

/// Ethernet ethertype for IPv4.
pub const ETHERTYPE_IPV4: u16 = 0x0800;

/// Ethernet ethertype for IPv6.
pub const ETHERTYPE_IPV6: u16 = 0x86dd;

/// Fully resolved egress data consumed by AF_XDP transmit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
