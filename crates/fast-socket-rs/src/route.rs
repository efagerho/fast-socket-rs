//! Route, neighbor, tunnel, and egress-resolution traits.

use core::fmt;

use crate::ip_packet::IpPacketEgress;
use crate::{IfIndex, IpFamily, Mixed};

/// Opaque route identifier for core egress handles and table implementations.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct RouteId(u64);

impl RouteId {
    /// Creates a route identifier from its raw value.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw route identifier.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Opaque neighbor identifier for core egress handles and table implementations.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct NeighborId(u64);

impl NeighborId {
    /// Creates a neighbor identifier from its raw value.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw neighbor identifier.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Opaque tunnel identifier for table implementations.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct TunnelId(u64);

impl TunnelId {
    /// Creates a tunnel identifier from its raw value.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw tunnel identifier.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Link-layer address.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct LinkAddr([u8; 6]);

impl LinkAddr {
    /// Creates a link-layer address from six octets.
    #[must_use]
    pub const fn new(octets: [u8; 6]) -> Self {
        Self(octets)
    }

    /// Returns the address octets.
    #[must_use]
    pub const fn octets(self) -> [u8; 6] {
        self.0
    }
}

impl fmt::Debug for LinkAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
            self.0[0], self.0[1], self.0[2], self.0[3], self.0[4], self.0[5]
        )
    }
}

impl fmt::Display for LinkAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

/// Error returned when parsing a `LinkAddr` from a `de:ad:be:ef:00:01`-style
/// MAC address string.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LinkAddrParseError {
    /// String did not have six `:`-separated octets.
    WrongOctetCount,
    /// One of the octets was not exactly two hex digits.
    InvalidOctet,
}

impl fmt::Display for LinkAddrParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongOctetCount => {
                f.write_str("MAC address must have six colon-separated octets")
            }
            Self::InvalidOctet => f.write_str("MAC address octet is not two hex digits"),
        }
    }
}

impl std::error::Error for LinkAddrParseError {}

impl core::str::FromStr for LinkAddr {
    type Err = LinkAddrParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut octets = [0u8; 6];
        let mut parts = value.split(':');
        for octet in octets.iter_mut() {
            let part = parts.next().ok_or(LinkAddrParseError::WrongOctetCount)?;
            if part.len() != 2 {
                return Err(LinkAddrParseError::InvalidOctet);
            }
            *octet = u8::from_str_radix(part, 16).map_err(|_| LinkAddrParseError::InvalidOctet)?;
        }
        if parts.next().is_some() {
            return Err(LinkAddrParseError::WrongOctetCount);
        }
        Ok(Self(octets))
    }
}

/// Result of routing an IP destination.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RouteHop<A> {
    /// Outgoing interface.
    pub ifindex: IfIndex,
    /// Next-hop IP address. For directly attached subnets this equals the destination.
    pub next_hop: A,
}

/// Tunnel target selected for transparent encapsulation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TunnelTarget<A> {
    /// Optional table-local tunnel identifier.
    pub id: Option<TunnelId>,
    /// Outer destination address.
    pub outer: A,
}

/// Destination-IP to route-hop lookup.
pub trait RouteTable<F: IpFamily = Mixed> {
    /// Resolves a destination IP into an outgoing interface and next hop.
    fn resolve_route(&self, dst: F::Addr) -> Option<RouteHop<F::Addr>>;
}

/// Next-hop-IP to link-layer-address lookup.
pub trait NeighborTable<F: IpFamily = Mixed> {
    /// Resolves a next-hop IP into a link-layer address.
    fn resolve_l2(&self, next_hop: F::Addr) -> Option<LinkAddr>;
}

/// Destination-IP to optional tunnel target lookup.
pub trait TunnelTable<Inner: IpFamily = Mixed, Outer: IpFamily = Mixed> {
    /// Resolves an inner destination into an optional outer tunnel destination.
    fn resolve_tunnel(&self, dst: Inner::Addr) -> Option<TunnelTarget<Outer::Addr>>;
}

/// Destination-IP to fully resolved backend egress lookup.
pub trait EgressResolver<F: IpFamily, E: IpPacketEgress> {
    /// Resolves a destination IP into the concrete egress value consumed by IP packet sends.
    fn resolve_egress(&self, dst: F::Addr) -> Option<E>;
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use super::*;
    use crate::{CoreEgress, V4Only};

    struct StaticRoute;

    impl RouteTable<V4Only> for StaticRoute {
        fn resolve_route(&self, dst: Ipv4Addr) -> Option<RouteHop<Ipv4Addr>> {
            Some(RouteHop {
                ifindex: IfIndex::new(2),
                next_hop: dst,
            })
        }
    }

    struct StaticNeighbor;

    impl NeighborTable<V4Only> for StaticNeighbor {
        fn resolve_l2(&self, _next_hop: Ipv4Addr) -> Option<LinkAddr> {
            Some(LinkAddr::new([0, 1, 2, 3, 4, 5]))
        }
    }

    struct Resolver<R, N> {
        route: R,
        neighbor: N,
    }

    impl<R, N> EgressResolver<V4Only, CoreEgress> for Resolver<R, N>
    where
        R: RouteTable<V4Only>,
        N: NeighborTable<V4Only>,
    {
        fn resolve_egress(&self, dst: Ipv4Addr) -> Option<CoreEgress> {
            let route = self.route.resolve_route(dst)?;
            let _link = self.neighbor.resolve_l2(route.next_hop)?;
            Some(CoreEgress::Neighbor(NeighborId::new(42)))
        }
    }

    #[test]
    fn resolver_composes_route_and_neighbor_tables() {
        let resolver = Resolver {
            route: StaticRoute,
            neighbor: StaticNeighbor,
        };

        assert_eq!(
            resolver.resolve_egress(Ipv4Addr::new(192, 0, 2, 1)),
            Some(CoreEgress::Neighbor(NeighborId::new(42)))
        );
    }
}
