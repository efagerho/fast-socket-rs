//! Queue-local route and neighbor snapshots for XDP egress resolution.

use std::collections::VecDeque;

use rustc_hash::FxHashMap;
use std::io;
use std::net::{IpAddr, Ipv4Addr};

use fast_socket_rs::{
    EgressResolver, IfIndex, LinkAddr, NeighborTable, QueueId, RouteHop, RouteTable, V4Only,
};

use crate::egress::XdpEgress;
use crate::netlink::{netlink_get_links, netlink_get_neighbors, netlink_get_routes};

/// Immutable route and neighbor snapshot.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RouteSnapshot {
    routes_v4: Vec<Ipv4Route>,
    neighbors_v4: FxHashMap<(IfIndex, Ipv4Addr), LinkAddr>,
    interfaces: FxHashMap<IfIndex, InterfaceInfo>,
}

/// IPv4 route entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Ipv4Route {
    /// Destination network address.
    pub destination: Ipv4Addr,
    /// Prefix length in bits.
    pub prefix_len: u8,
    /// Output interface.
    pub ifindex: IfIndex,
    /// Optional gateway; direct routes use the destination as next hop.
    pub gateway: Option<Ipv4Addr>,
    /// Route priority. Lower is preferred for equal prefix length.
    pub priority: u32,
    /// Effective route MTU.
    pub mtu: u32,
}

/// Interface facts used when resolving XDP egress.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InterfaceInfo {
    /// Interface index.
    pub ifindex: IfIndex,
    /// Master interface index when this interface is enslaved to another link.
    pub master_ifindex: Option<IfIndex>,
    /// Source MAC address.
    pub mac: LinkAddr,
    /// Interface MTU.
    pub mtu: u32,
    /// Preferred queue for this interface.
    pub queue: QueueId,
}

impl RouteSnapshot {
    /// Creates an empty snapshot.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Builds a snapshot from Linux netlink route, neighbor, and link dumps.
    ///
    /// The snapshot is queue-agnostic; callers that want per-queue egress
    /// handles should resolve them through
    /// [`XdpLocalRoutes::egress_v4_for_interface`], which accepts a queue id
    /// at the call site. The default-queue stamp on the underlying
    /// [`InterfaceInfo`] entries is only used by the simpler
    /// [`XdpLocalRoutes::egress_v4`] convenience and is informational
    /// otherwise.
    pub fn from_netlink() -> io::Result<Self> {
        Self::from_netlink_table(u32::from(libc::RT_TABLE_MAIN))
    }

    /// Like [`Self::from_netlink`] but uses an explicit route table id.
    pub fn from_netlink_table(table: u32) -> io::Result<Self> {
        // Stamp every interface with queue 0; queue-aware lookups go through
        // `egress_v4_for_interface` and override this value.
        let queue = QueueId::new(0);
        let _ = queue; // silence warning if upstream renames the field
        Self::build_netlink_snapshot(queue, table)
    }

    fn build_netlink_snapshot(queue: QueueId, table: u32) -> io::Result<Self> {
        let mut snapshot = Self::new();

        for link in netlink_get_links(libc::AF_UNSPEC as u8)? {
            if let Some(mac) = link.mac {
                snapshot.upsert_interface(InterfaceInfo {
                    ifindex: link.ifindex,
                    master_ifindex: link.master_ifindex,
                    mac,
                    mtu: link.mtu.unwrap_or(1500),
                    queue,
                });
            }
        }

        // Bulk-insert without re-sorting between each route; the sort is
        // amortized to a single O(n log n) pass at the end.
        snapshot.upsert_routes_v4(netlink_get_routes(libc::AF_INET as u8, table)?.into_iter().filter_map(
            |route| {
                let ifindex = route.out_ifindex?;
                let destination = match route.destination {
                    Some(IpAddr::V4(destination)) => destination,
                    None => Ipv4Addr::UNSPECIFIED,
                    Some(IpAddr::V6(_)) => return None,
                };
                let gateway = match route.gateway {
                    Some(IpAddr::V4(gateway)) => Some(gateway),
                    None => None,
                    Some(IpAddr::V6(_)) => return None,
                };
                Some(Ipv4Route {
                    destination,
                    prefix_len: route.dst_len.min(32),
                    ifindex,
                    gateway,
                    priority: route.priority.unwrap_or(u32::MAX),
                    mtu: u32::MAX,
                })
            },
        ));

        for neighbor in netlink_get_neighbors(None, libc::AF_INET as u8)? {
            let (Some(IpAddr::V4(ip)), Some(mac)) = (neighbor.destination, neighbor.lladdr) else {
                continue;
            };
            snapshot.upsert_neighbor_v4(neighbor.ifindex, ip, mac);
        }

        Ok(snapshot)
    }

    /// Adds or replaces an IPv4 route. Sorts the route table after every
    /// insert, so prefer [`Self::upsert_routes_v4`] when bulk-loading.
    pub fn upsert_route_v4(&mut self, route: Ipv4Route) {
        self.upsert_route_v4_no_sort(route);
        self.sort_routes_v4();
    }

    /// Adds or replaces many IPv4 routes, sorting the route table only once
    /// at the end. O(n) inserts + a single O(n log n) sort instead of N×
    /// O(n log n) re-sorts; preferable for any bulk loader (netlink dumps,
    /// route-monitor snapshots).
    pub fn upsert_routes_v4<I>(&mut self, routes: I)
    where
        I: IntoIterator<Item = Ipv4Route>,
    {
        let mut any = false;
        for route in routes {
            self.upsert_route_v4_no_sort(route);
            any = true;
        }
        if any {
            self.sort_routes_v4();
        }
    }

    fn upsert_route_v4_no_sort(&mut self, route: Ipv4Route) {
        if let Some(existing) = self.routes_v4.iter_mut().find(|existing| {
            existing.destination == route.destination
                && existing.prefix_len == route.prefix_len
                && existing.ifindex == route.ifindex
                && existing.gateway == route.gateway
        }) {
            *existing = route;
        } else {
            self.routes_v4.push(route);
        }
    }

    fn sort_routes_v4(&mut self) {
        self.routes_v4.sort_by(|left, right| {
            right
                .prefix_len
                .cmp(&left.prefix_len)
                .then(left.priority.cmp(&right.priority))
        });
    }

    /// Adds or replaces an IPv4 neighbor entry.
    pub fn upsert_neighbor_v4(&mut self, ifindex: IfIndex, ip: Ipv4Addr, mac: LinkAddr) {
        self.neighbors_v4.insert((ifindex, ip), mac);
    }

    /// Adds or replaces interface facts.
    pub fn upsert_interface(&mut self, interface: InterfaceInfo) {
        self.interfaces.insert(interface.ifindex, interface);
    }

    /// Resolves an IPv4 route.
    #[must_use]
    pub fn route_v4(&self, dst: Ipv4Addr) -> Option<RouteHop<Ipv4Addr>> {
        let route = self.lookup_route_v4(dst)?;
        Some(RouteHop {
            ifindex: route.ifindex,
            next_hop: route.gateway.unwrap_or(dst),
        })
    }

    /// Resolves an IPv4 egress handle.
    #[must_use]
    pub fn egress_v4(&self, dst: Ipv4Addr) -> Option<XdpEgress> {
        let route = self.lookup_route_v4(dst)?;
        let next_hop = route.gateway.unwrap_or(dst);
        let dst_mac = self.neighbors_v4.get(&(route.ifindex, next_hop)).copied()?;
        let interface = self.interfaces.get(&route.ifindex).copied()?;
        Some(XdpEgress::ipv4(
            route.ifindex,
            interface.queue,
            dst_mac,
            interface.mac,
            route.mtu.min(interface.mtu),
        ))
    }

    /// Resolves an IPv4 egress handle for one queue-local AF_XDP interface.
    ///
    /// This permits attaching AF_XDP to a physical slave when Linux routes
    /// through its bond master, while preserving normal route/interface matching
    /// for non-enslaved links.
    #[must_use]
    pub fn egress_v4_for_interface(
        &self,
        dst: Ipv4Addr,
        ifindex: IfIndex,
        queue: QueueId,
    ) -> Option<XdpEgress> {
        let route = self.lookup_route_v4(dst)?;
        let interface = self.interfaces.get(&ifindex).copied()?;
        if route.ifindex != ifindex && interface.master_ifindex != Some(route.ifindex) {
            return None;
        }

        let next_hop = route.gateway.unwrap_or(dst);
        let dst_mac = self
            .neighbors_v4
            .get(&(ifindex, next_hop))
            .or_else(|| self.neighbors_v4.get(&(route.ifindex, next_hop)))
            .copied()?;
        Some(XdpEgress::ipv4(
            ifindex,
            queue,
            dst_mac,
            interface.mac,
            route.mtu.min(interface.mtu),
        ))
    }

    fn lookup_route_v4(&self, dst: Ipv4Addr) -> Option<Ipv4Route> {
        let dst = u32::from(dst);
        self.routes_v4
            .iter()
            .copied()
            .find(|route| prefix_matches(dst, route.destination, route.prefix_len))
    }
}

impl RouteTable<V4Only> for RouteSnapshot {
    fn resolve_route(&self, dst: Ipv4Addr) -> Option<RouteHop<Ipv4Addr>> {
        self.route_v4(dst)
    }
}

impl NeighborTable<V4Only> for RouteSnapshot {
    fn resolve_l2(&self, next_hop: Ipv4Addr) -> Option<LinkAddr> {
        self.neighbors_v4
            .iter()
            .find_map(|((_ifindex, ip), mac)| (*ip == next_hop).then_some(*mac))
    }
}

impl EgressResolver<V4Only, XdpEgress> for RouteSnapshot {
    fn resolve_egress(&self, dst: Ipv4Addr) -> Option<XdpEgress> {
        self.egress_v4(dst)
    }
}

/// Queue-local route state with cold-path update adoption.
#[derive(Clone, Debug)]
pub struct XdpLocalRoutes {
    snapshot: Box<RouteSnapshot>,
    pending: VecDeque<RouteSnapshot>,
}

impl XdpLocalRoutes {
    /// Creates local routes from an initial snapshot.
    #[must_use]
    pub fn new(snapshot: RouteSnapshot) -> Self {
        Self {
            snapshot: Box::new(snapshot),
            pending: VecDeque::new(),
        }
    }

    /// Returns the currently adopted snapshot.
    #[must_use]
    pub fn snapshot(&self) -> &RouteSnapshot {
        &self.snapshot
    }

    /// Queues a cold-path snapshot update.
    pub fn push_update(&mut self, snapshot: RouteSnapshot) {
        self.pending.push_back(snapshot);
    }

    /// Applies queued updates outside the packet path.
    pub fn apply_updates(&mut self) -> usize {
        let mut applied = 0;
        while let Some(snapshot) = self.pending.pop_front() {
            *self.snapshot = snapshot;
            applied += 1;
        }
        applied
    }

    /// Resolves an IPv4 egress from queue-local immutable memory.
    #[inline]
    #[must_use]
    pub fn resolve_v4(&self, dst: Ipv4Addr) -> Option<XdpEgress> {
        self.snapshot.egress_v4(dst)
    }

    /// Resolves IPv4 egress for one queue-local AF_XDP interface.
    #[inline]
    #[must_use]
    pub fn resolve_v4_for_interface(
        &self,
        dst: Ipv4Addr,
        ifindex: IfIndex,
        queue: QueueId,
    ) -> Option<XdpEgress> {
        self.snapshot.egress_v4_for_interface(dst, ifindex, queue)
    }
}

impl Default for XdpLocalRoutes {
    fn default() -> Self {
        Self::new(RouteSnapshot::new())
    }
}

impl EgressResolver<V4Only, XdpEgress> for XdpLocalRoutes {
    fn resolve_egress(&self, dst: Ipv4Addr) -> Option<XdpEgress> {
        self.resolve_v4(dst)
    }
}

fn prefix_matches(dst: u32, network: Ipv4Addr, prefix_len: u8) -> bool {
    if prefix_len == 0 {
        return true;
    }
    let prefix_len = prefix_len.min(32);
    let mask = u32::MAX << (32 - prefix_len);
    (dst & mask) == (u32::from(network) & mask)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mac(value: u8) -> LinkAddr {
        LinkAddr::new([value; 6])
    }

    #[test]
    fn route_snapshot_resolves_longest_prefix_egress() {
        let mut snapshot = RouteSnapshot::new();
        snapshot.upsert_interface(InterfaceInfo {
            ifindex: IfIndex::new(2),
            master_ifindex: None,
            mac: mac(1),
            mtu: 1500,
            queue: QueueId::new(7),
        });
        snapshot.upsert_neighbor_v4(IfIndex::new(2), Ipv4Addr::new(192, 0, 2, 99), mac(9));
        snapshot.upsert_route_v4(Ipv4Route {
            destination: Ipv4Addr::new(0, 0, 0, 0),
            prefix_len: 0,
            ifindex: IfIndex::new(2),
            gateway: Some(Ipv4Addr::new(192, 0, 2, 99)),
            priority: 100,
            mtu: 1400,
        });

        let egress = snapshot
            .egress_v4(Ipv4Addr::new(198, 51, 100, 10))
            .expect("default route resolves");
        assert_eq!(egress.queue, QueueId::new(7));
        assert_eq!(egress.mtu, 1400);
        assert_eq!(egress.dst_mac, mac(9));
    }

    #[test]
    fn route_snapshot_resolves_bond_master_route_on_slave_interface() {
        let mut snapshot = RouteSnapshot::new();
        snapshot.upsert_interface(InterfaceInfo {
            ifindex: IfIndex::new(2),
            master_ifindex: Some(IfIndex::new(4)),
            mac: mac(1),
            mtu: 1500,
            queue: QueueId::new(0),
        });
        snapshot.upsert_interface(InterfaceInfo {
            ifindex: IfIndex::new(4),
            master_ifindex: None,
            mac: mac(2),
            mtu: 1500,
            queue: QueueId::new(0),
        });
        snapshot.upsert_neighbor_v4(IfIndex::new(4), Ipv4Addr::new(192, 0, 2, 99), mac(9));
        snapshot.upsert_route_v4(Ipv4Route {
            destination: Ipv4Addr::new(0, 0, 0, 0),
            prefix_len: 0,
            ifindex: IfIndex::new(4),
            gateway: Some(Ipv4Addr::new(192, 0, 2, 99)),
            priority: 100,
            mtu: 1400,
        });

        let egress = snapshot
            .egress_v4_for_interface(
                Ipv4Addr::new(198, 51, 100, 10),
                IfIndex::new(2),
                QueueId::new(7),
            )
            .expect("bond master route resolves through slave");
        assert_eq!(egress.ifindex, IfIndex::new(2));
        assert_eq!(egress.queue, QueueId::new(7));
        assert_eq!(egress.src_mac, mac(1));
        assert_eq!(egress.dst_mac, mac(9));
    }

    #[test]
    fn local_routes_apply_updates_off_hot_path() {
        let mut local = XdpLocalRoutes::default();
        let mut snapshot = RouteSnapshot::new();
        snapshot.upsert_interface(InterfaceInfo {
            ifindex: IfIndex::new(1),
            master_ifindex: None,
            mac: mac(1),
            mtu: 1500,
            queue: QueueId::new(0),
        });
        local.push_update(snapshot);
        assert_eq!(local.apply_updates(), 1);
        assert_eq!(local.apply_updates(), 0);
    }
}
