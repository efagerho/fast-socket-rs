//! Queue-local route and neighbor snapshots for XDP egress resolution.

use std::collections::VecDeque;

use rustc_hash::FxHashMap;
use std::io;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use fast_socket_rs::{
    EgressResolver, IfIndex, LinkAddr, NeighborTable, QueueId, RouteHop, RouteTable, V4Only,
};
use poptrie::Ipv4Poptrie;

use crate::egress::{ResolvedL2, XdpEgress, XdpResolvedEgress, build_ethernet_header};
use crate::netlink::{netlink_get_links, netlink_get_neighbors, netlink_get_routes};

/// Immutable route and neighbor snapshot.
///
/// IPv4 gateway routes also carry precomputed AF_XDP egress data, including
/// L2 header bytes, rebuilt when route, neighbor, or interface facts change.
#[derive(Clone, Debug, Default)]
pub struct RouteSnapshot {
    routes_v4: Vec<Ipv4Route>,
    /// Longest-prefix-match index mapping a destination to an index into
    /// `routes_v4`. Rebuilt only when routes change; `None` when there are no
    /// routes. Held behind an `Arc` so cloning a snapshot per queue shares the
    /// (potentially large) index instead of duplicating it.
    route_index_v4: Option<Arc<Ipv4Poptrie<u32>>>,
    /// Folded egress index: for each egress interface, the resolved egress of
    /// every route, indexed by the **same route index the poptrie returns**.
    /// This collapses destination → route → egress into one poptrie lookup plus
    /// a dense array index, replacing a second per-packet composite-key hashmap
    /// probe. Arc-shared so snapshot clones stay cheap; rebuilt when routes,
    /// neighbors, or interfaces change.
    egress_index_v4: Arc<FxHashMap<IfIndex, InterfaceEgress>>,
    neighbors_v4: FxHashMap<(IfIndex, Ipv4Addr), LinkAddr>,
    interfaces: FxHashMap<IfIndex, InterfaceInfo>,
}

/// Per-interface folded egress: `slots[route_index]` resolves a route (the
/// index the poptrie returns for a destination) to its egress on this
/// interface, with no second hashmap probe.
#[derive(Clone, Debug)]
struct InterfaceEgress {
    /// One entry per route, in `routes_v4` order: [`EGRESS_NONE`] when the
    /// route does not egress this interface, [`EGRESS_ON_LINK`] for a direct
    /// route (the next hop is the destination, so its neighbor MAC is resolved
    /// per packet), otherwise an index into `egresses`.
    slots: Box<[u32]>,
    /// Distinct fully-resolved gateway egresses (prebuilt L2 header),
    /// deduplicated. The socket's queue is stamped per send via `with_queue`.
    egresses: Box<[XdpResolvedEgress]>,
}

/// `slots` sentinel: the route does not egress this interface.
const EGRESS_NONE: u32 = u32::MAX;
/// `slots` sentinel: direct (on-link) route — the next hop is the destination,
/// so its neighbor MAC is resolved per packet rather than prebuilt.
const EGRESS_ON_LINK: u32 = u32::MAX - 1;

impl PartialEq for RouteSnapshot {
    fn eq(&self, other: &Self) -> bool {
        // `route_index_v4` and `egress_index_v4` are pure functions of the
        // source fields below, so comparing the inputs is sufficient — and
        // avoids comparing the large derived indexes.
        self.routes_v4 == other.routes_v4
            && self.neighbors_v4 == other.neighbors_v4
            && self.interfaces == other.interfaces
    }
}

impl Eq for RouteSnapshot {}

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
        Self::build_netlink_snapshot(QueueId::new(0), table)
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
        snapshot.upsert_routes_v4(
            netlink_get_routes(libc::AF_INET as u8, table)?
                .into_iter()
                .filter_map(|route| {
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
                        // Per-route MTU lives in the nested RTA_METRICS/RTAX_MTU
                        // attribute, which the netlink parser does not read; `MAX`
                        // means "no per-route cap", so egress falls back to the
                        // interface MTU (`route.mtu.min(interface.mtu)`). A lower
                        // PMTU pinned on a specific route is therefore not honored.
                        mtu: u32::MAX,
                    })
                }),
        );

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
        self.rebuild_route_index_v4();
        self.rebuild_egress_index_v4();
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
            self.rebuild_route_index_v4();
            self.rebuild_egress_index_v4();
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
        self.rebuild_egress_index_v4();
    }

    /// Adds or replaces interface facts.
    pub fn upsert_interface(&mut self, interface: InterfaceInfo) {
        self.interfaces.insert(interface.ifindex, interface);
        self.rebuild_egress_index_v4();
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
        self.resolved_v4_for_interface(dst, ifindex, queue)
            .map(|resolved| resolved.egress())
    }

    pub(crate) fn resolved_v4_for_interface(
        &self,
        dst: Ipv4Addr,
        ifindex: IfIndex,
        queue: QueueId,
    ) -> Option<XdpResolvedEgress> {
        // One poptrie lookup yields the route index; the per-interface `slots`
        // array resolves it to an egress with a dense index — no second
        // composite-key hashmap probe on the send path.
        let route_index = self.lookup_route_index_v4(dst)?;
        let interface_egress = self.egress_index_v4.get(&ifindex)?;
        match *interface_egress.slots.get(route_index as usize)? {
            EGRESS_NONE => None,
            EGRESS_ON_LINK => {
                // Direct route: resolve the destination's own neighbor MAC.
                let route = self.routes_v4.get(route_index as usize).copied()?;
                let interface = self.interfaces.get(&ifindex).copied()?;
                let dst_mac = self
                    .neighbors_v4
                    .get(&(ifindex, dst))
                    .or_else(|| self.neighbors_v4.get(&(route.ifindex, dst)))
                    .copied()?;
                Some(XdpResolvedEgress::from_egress(XdpEgress::ipv4(
                    ifindex,
                    queue,
                    dst_mac,
                    interface.mac,
                    route.mtu.min(interface.mtu),
                )))
            }
            gateway_slot => {
                Some(interface_egress.egresses[gateway_slot as usize].with_queue(queue))
            }
        }
    }

    /// Transmit hot-path resolution: returns only the L2 header and effective
    /// MTU, **borrowing** the prebuilt header for gateway routes instead of
    /// copying a full egress and stamping the queue.
    ///
    /// The caller's `(ifindex, queue)` are fixed and the egress stored in
    /// `egress_index_v4` for `ifindex` is already correct for it, so no queue
    /// stamping or revalidation is needed — unlike the general
    /// [`Self::resolved_v4_for_interface`], which custom routers and the
    /// `egress_v4_for_interface` API still use.
    #[inline]
    pub(crate) fn resolve_l2_for_interface(
        &self,
        dst: Ipv4Addr,
        ifindex: IfIndex,
        mtu: usize,
    ) -> Option<ResolvedL2<'_>> {
        let route_index = self.lookup_route_index_v4(dst)?;
        let interface_egress = self.egress_index_v4.get(&ifindex)?;
        match *interface_egress.slots.get(route_index as usize)? {
            EGRESS_NONE => None,
            // On-link destinations build a fresh header (neighbor lookup +
            // `build_ethernet_header`); keep that out of line so this resolver
            // stays small enough to inline into the per-packet send path. The
            // borrow (gateway) arm below is the hot path.
            EGRESS_ON_LINK => self.resolve_l2_on_link_v4(route_index, dst, ifindex, mtu),
            gateway_slot => {
                let resolved = &interface_egress.egresses[gateway_slot as usize];
                Some(ResolvedL2::Borrowed {
                    l2_header: resolved.l2_header(),
                    ip_mtu: mtu.min(resolved.mtu() as usize),
                })
            }
        }
    }

    /// Cold on-link path of [`Self::resolve_l2_for_interface`]: resolves the
    /// destination's own neighbor MAC and builds the Ethernet header inline.
    /// Kept out of line (`#[inline(never)]`) so the gateway hot path stays
    /// small and inlinable.
    #[inline(never)]
    fn resolve_l2_on_link_v4(
        &self,
        route_index: u32,
        dst: Ipv4Addr,
        ifindex: IfIndex,
        mtu: usize,
    ) -> Option<ResolvedL2<'_>> {
        let route = self.routes_v4.get(route_index as usize).copied()?;
        let interface = self.interfaces.get(&ifindex).copied()?;
        let dst_mac = self
            .neighbors_v4
            .get(&(ifindex, dst))
            .or_else(|| self.neighbors_v4.get(&(route.ifindex, dst)))
            .copied()?;
        let egress = XdpEgress::ipv4(
            ifindex,
            interface.queue,
            dst_mac,
            interface.mac,
            route.mtu.min(interface.mtu),
        );
        let (l2_header, l2_len) = build_ethernet_header(egress);
        Some(ResolvedL2::Inline {
            l2_header,
            l2_len,
            ip_mtu: mtu.min(egress.mtu as usize),
        })
    }

    /// Returns the index into `routes_v4` of the longest-prefix match, via the
    /// poptrie. This index is what `egress_index_v4`'s per-interface `slots`
    /// are keyed on, so the send path resolves egress with a single lookup.
    fn lookup_route_index_v4(&self, dst: Ipv4Addr) -> Option<u32> {
        self.route_index_v4.as_ref()?.lookup(u32::from(dst)).copied()
    }

    fn lookup_route_v4(&self, dst: Ipv4Addr) -> Option<Ipv4Route> {
        let index = self.lookup_route_index_v4(dst)?;
        self.routes_v4.get(index as usize).copied()
    }

    /// Rebuilds the longest-prefix-match index. Call after `routes_v4` changes
    /// (and after it has been sorted); not needed for neighbor/interface
    /// updates, which leave the route set untouched.
    fn rebuild_route_index_v4(&mut self) {
        if self.routes_v4.is_empty() {
            self.route_index_v4 = None;
            return;
        }
        let mut builder = Ipv4Poptrie::builder();
        // `routes_v4` is sorted longest-prefix-first, then lowest-priority-first.
        // Inserting in reverse means that for an identical (destination,
        // prefix_len) the entry the old linear scan would have returned (the
        // first in sorted order — longest prefix, lowest priority) is inserted
        // last and therefore wins. Longest-prefix-match across *different*
        // prefix lengths is handled by the trie structure itself.
        for (index, route) in self.routes_v4.iter().enumerate().rev() {
            builder.insert(
                u32::from(route.destination),
                route.prefix_len.min(32),
                index as u32,
            );
        }
        self.route_index_v4 = Some(Arc::new(builder.build()));
    }

    /// Rebuilds the folded per-interface egress index. Must run after
    /// `rebuild_route_index_v4` on route changes (the `slots` are keyed by the
    /// poptrie's route index) and also on neighbor/interface changes (which
    /// change resolved MACs without changing route indices).
    ///
    /// Each egress interface gets a dense `slots` array (one entry per route).
    /// An interface with no egressable route is omitted, so a host that routes
    /// everything through one NIC pays for a single dense array, not one per
    /// interface.
    fn rebuild_egress_index_v4(&mut self) {
        let route_count = self.routes_v4.len();
        if route_count == 0 {
            self.egress_index_v4 = Arc::new(FxHashMap::default());
            return;
        }

        let mut index: FxHashMap<IfIndex, InterfaceEgress> = FxHashMap::default();
        for interface in self.interfaces.values().copied() {
            let mut slots = vec![EGRESS_NONE; route_count];
            let mut egresses: Vec<XdpResolvedEgress> = Vec::new();
            let mut dedup: FxHashMap<XdpResolvedEgress, u32> = FxHashMap::default();
            let mut egressable = false;

            for (route_index, route) in self.routes_v4.iter().enumerate() {
                // Does this route egress through `interface` directly, or via it
                // as the bond master of an enslaved `interface`?
                if route.ifindex != interface.ifindex
                    && interface.master_ifindex != Some(route.ifindex)
                {
                    continue;
                }

                match route.gateway {
                    Some(gateway) => {
                        // Gateway route: the next hop (the gateway) is fixed for
                        // the whole prefix, so its egress is prebuilt once here.
                        let Some(dst_mac) = self
                            .neighbors_v4
                            .get(&(interface.ifindex, gateway))
                            .or_else(|| self.neighbors_v4.get(&(route.ifindex, gateway)))
                            .copied()
                        else {
                            continue; // gateway MAC unknown -> leaves EGRESS_NONE
                        };
                        let resolved = XdpResolvedEgress::from_egress(XdpEgress::ipv4(
                            interface.ifindex,
                            interface.queue,
                            dst_mac,
                            interface.mac,
                            route.mtu.min(interface.mtu),
                        ));
                        let slot = *dedup.entry(resolved).or_insert_with(|| {
                            let slot = egresses.len() as u32;
                            egresses.push(resolved);
                            slot
                        });
                        slots[route_index] = slot;
                        egressable = true;
                    }
                    None => {
                        // Direct route: next hop is the destination, so the
                        // neighbor MAC varies per packet -> resolve at send time.
                        slots[route_index] = EGRESS_ON_LINK;
                        egressable = true;
                    }
                }
            }

            if egressable {
                index.insert(
                    interface.ifindex,
                    InterfaceEgress {
                        slots: slots.into_boxed_slice(),
                        egresses: egresses.into_boxed_slice(),
                    },
                );
            }
        }
        self.egress_index_v4 = Arc::new(index);
    }
}

impl RouteTable<V4Only> for RouteSnapshot {
    fn resolve_route(&self, dst: Ipv4Addr) -> Option<RouteHop<Ipv4Addr>> {
        self.route_v4(dst)
    }
}

impl NeighborTable<V4Only> for RouteSnapshot {
    /// Best-effort L2 resolution by next-hop IP **only**.
    ///
    /// The `NeighborTable` trait has no interface parameter, so on a multi-homed
    /// host where the same next-hop IP exists on more than one interface this
    /// returns the first matching MAC across any interface and may pick the
    /// wrong one. The AF_XDP transmit path does not use this method; it resolves
    /// through the interface-keyed [`Self::egress_v4_for_interface`], which
    /// disambiguates by `(ifindex, next_hop)`. Prefer that for queue-local
    /// egress; this impl exists only for generic `NeighborTable` consumers.
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
    #[inline]
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

    #[inline]
    pub(crate) fn resolve_v4_resolved_for_interface(
        &self,
        dst: Ipv4Addr,
        ifindex: IfIndex,
        queue: QueueId,
    ) -> Option<XdpResolvedEgress> {
        self.snapshot.resolved_v4_for_interface(dst, ifindex, queue)
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
    fn route_snapshot_precomputes_gateway_l2_header_and_updates_on_neighbor_change() {
        let mut snapshot = RouteSnapshot::new();
        snapshot.upsert_interface(InterfaceInfo {
            ifindex: IfIndex::new(2),
            master_ifindex: None,
            mac: mac(1),
            mtu: 1500,
            queue: QueueId::new(0),
        });
        snapshot.upsert_route_v4(Ipv4Route {
            destination: Ipv4Addr::new(0, 0, 0, 0),
            prefix_len: 0,
            ifindex: IfIndex::new(2),
            gateway: Some(Ipv4Addr::new(192, 0, 2, 99)),
            priority: 100,
            mtu: 1500,
        });
        snapshot.upsert_neighbor_v4(IfIndex::new(2), Ipv4Addr::new(192, 0, 2, 99), mac(9));

        let first = snapshot
            .resolved_v4_for_interface(
                Ipv4Addr::new(198, 51, 100, 10),
                IfIndex::new(2),
                QueueId::new(7),
            )
            .expect("gateway route resolves");
        assert_eq!(&first.l2_header()[..6], &mac(9).octets());

        snapshot.upsert_neighbor_v4(IfIndex::new(2), Ipv4Addr::new(192, 0, 2, 99), mac(10));
        let updated = snapshot
            .resolved_v4_for_interface(
                Ipv4Addr::new(198, 51, 100, 10),
                IfIndex::new(2),
                QueueId::new(7),
            )
            .expect("gateway route resolves after neighbor update");
        assert_eq!(&updated.l2_header()[..6], &mac(10).octets());
        assert_eq!(updated.egress().queue, QueueId::new(7));
    }

    #[test]
    fn route_snapshot_selects_longest_prefix_then_lowest_priority() {
        // Exercises the poptrie-backed `lookup_route_v4`: longest prefix wins
        // across lengths, and among routes sharing an exact prefix the lowest
        // priority wins (matching the old sorted linear-scan behavior).
        let mut snapshot = RouteSnapshot::new();
        snapshot.upsert_route_v4(Ipv4Route {
            destination: Ipv4Addr::new(10, 0, 0, 0),
            prefix_len: 8,
            ifindex: IfIndex::new(2),
            gateway: None,
            priority: 100,
            mtu: 1500,
        });
        snapshot.upsert_route_v4(Ipv4Route {
            destination: Ipv4Addr::new(10, 1, 2, 0),
            prefix_len: 24,
            ifindex: IfIndex::new(3),
            gateway: None,
            priority: 100,
            mtu: 1500,
        });
        // Same /24 prefix, different interface, lower priority — should win.
        snapshot.upsert_route_v4(Ipv4Route {
            destination: Ipv4Addr::new(10, 1, 2, 0),
            prefix_len: 24,
            ifindex: IfIndex::new(9),
            gateway: None,
            priority: 50,
            mtu: 1500,
        });

        let hop = snapshot
            .route_v4(Ipv4Addr::new(10, 1, 2, 200))
            .expect("longest prefix match");
        assert_eq!(hop.ifindex, IfIndex::new(9), "lowest priority /24 wins");

        let hop = snapshot
            .route_v4(Ipv4Addr::new(10, 9, 9, 9))
            .expect("falls back to the covering /8");
        assert_eq!(hop.ifindex, IfIndex::new(2));

        assert!(
            snapshot.route_v4(Ipv4Addr::new(11, 0, 0, 1)).is_none(),
            "no route outside the /8"
        );
    }

    #[test]
    fn resolve_l2_for_interface_borrows_gateway_and_builds_on_link() {
        let mut snapshot = RouteSnapshot::new();
        snapshot.upsert_interface(InterfaceInfo {
            ifindex: IfIndex::new(2),
            master_ifindex: None,
            mac: mac(1),
            mtu: 1500,
            queue: QueueId::new(0),
        });
        // Default route via a gateway (prebuilt egress).
        snapshot.upsert_route_v4(Ipv4Route {
            destination: Ipv4Addr::new(0, 0, 0, 0),
            prefix_len: 0,
            ifindex: IfIndex::new(2),
            gateway: Some(Ipv4Addr::new(192, 0, 2, 99)),
            priority: 100,
            mtu: 1400,
        });
        snapshot.upsert_neighbor_v4(IfIndex::new(2), Ipv4Addr::new(192, 0, 2, 99), mac(9));
        // On-link /8 (direct route; per-destination neighbor).
        snapshot.upsert_route_v4(Ipv4Route {
            destination: Ipv4Addr::new(10, 0, 0, 0),
            prefix_len: 8,
            ifindex: IfIndex::new(2),
            gateway: None,
            priority: 100,
            mtu: 1500,
        });
        snapshot.upsert_neighbor_v4(IfIndex::new(2), Ipv4Addr::new(10, 1, 2, 3), mac(5));

        // Gateway destination: header borrowed from the prebuilt egress.
        let gateway = snapshot
            .resolve_l2_for_interface(Ipv4Addr::new(8, 8, 8, 8), IfIndex::new(2), 1500)
            .expect("gateway route resolves");
        assert!(matches!(gateway, ResolvedL2::Borrowed { .. }));
        assert_eq!(&gateway.l2_header()[..6], &mac(9).octets()); // dst MAC = gateway
        assert_eq!(&gateway.l2_header()[6..12], &mac(1).octets()); // src MAC = interface
        assert_eq!(gateway.ip_mtu(), 1400);

        // On-link destination: header built inline from the destination's own
        // neighbor entry.
        let on_link = snapshot
            .resolve_l2_for_interface(Ipv4Addr::new(10, 1, 2, 3), IfIndex::new(2), 1500)
            .expect("on-link route resolves");
        assert!(matches!(on_link, ResolvedL2::Inline { .. }));
        assert_eq!(&on_link.l2_header()[..6], &mac(5).octets());
        assert_eq!(on_link.ip_mtu(), 1500);

        // On-link destination with no neighbor entry: no egress.
        assert!(
            snapshot
                .resolve_l2_for_interface(Ipv4Addr::new(10, 9, 9, 9), IfIndex::new(2), 1500)
                .is_none()
        );

        // Updating the gateway's neighbor is reflected in the borrowed header.
        snapshot.upsert_neighbor_v4(IfIndex::new(2), Ipv4Addr::new(192, 0, 2, 99), mac(10));
        let updated = snapshot
            .resolve_l2_for_interface(Ipv4Addr::new(8, 8, 8, 8), IfIndex::new(2), 1500)
            .expect("gateway route resolves after neighbor update");
        assert_eq!(&updated.l2_header()[..6], &mac(10).octets());
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
