//! Single route-monitor fanout for XDP queues.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use arc_swap::ArcSwapOption;
use fast_socket_rs::QueueId;

use crate::route::{RouteSnapshot, XdpLocalRoutes};

/// A published snapshot stamped with a monotonically increasing generation.
///
/// Subscribers compare the generation rather than the `Arc` pointer: a pointer
/// can be reused (ABA) when an old snapshot is freed and a new one is allocated
/// at the same address, which would make a real update look unchanged. The
/// generation never repeats, so the comparison is unambiguous.
#[derive(Debug)]
struct VersionedSnapshot {
    generation: u64,
    snapshot: RouteSnapshot,
}

/// Latest published snapshot. A single allocation is shared by every
/// subscriber via `Arc<ArcSwapOption<VersionedSnapshot>>`. The publisher
/// atomically swaps a new generation-stamped snapshot in; subscribers do a
/// lock-free `load` and compare the generation against their last observation.
/// Older snapshots are never queued — they have no value.
type SharedSnapshot = Arc<ArcSwapOption<VersionedSnapshot>>;

/// Handle used by queue owners to receive cold-path route updates.
///
/// Holds a clone of the shared `ArcSwapOption` plus the generation of the last
/// snapshot this handle applied. `apply_updates` is a lock-free
/// load-and-generation-compare on the hot path; no `Mutex` or `mpsc` is
/// involved. The handle holds only `Send + Sync` fields, so it is `Send`
/// without an `unsafe impl`.
#[derive(Debug)]
pub struct XdpRouteMonitorHandle {
    shared: SharedSnapshot,
    /// Generation of the last snapshot this handle observed. `0` means "no
    /// snapshot yet" (published generations start at `1`).
    last_seen_generation: u64,
}

impl XdpRouteMonitorHandle {
    /// Installs the latest published snapshot (if any) into `routes` and
    /// returns the number of `XdpLocalRoutes` updates applied.
    ///
    /// Lock-free: a single `ArcSwapOption::load` plus a generation compare. If
    /// the generation matches what we last saw, nothing has been published
    /// since our last call and we return zero.
    pub fn apply_updates(&mut self, routes: &mut XdpLocalRoutes) -> usize {
        let Some(versioned) = self.shared.load_full() else {
            return 0;
        };
        if self.last_seen_generation == versioned.generation {
            return 0;
        }
        self.last_seen_generation = versioned.generation;
        // The hot packet path never sees the `Arc`; clone the contents once
        // into the queue-local table.
        routes.push_update(versioned.snapshot.clone());
        routes.apply_updates()
    }
}

/// One route monitor fanout serving multiple queue owners.
///
/// The previous design used one `mpsc::channel` per subscriber, which queued
/// every published snapshot per subscriber (unbounded). Older snapshots are
/// never useful — a subscriber that wakes up late always wants the *latest*
/// state. This implementation collapses the queue to a single
/// `Arc<ArcSwapOption<RouteSnapshot>>`: the publisher atomically swaps a new
/// `Arc<RouteSnapshot>` into the shared slot, and every subscriber sees it
/// on its next `apply_updates` call.
#[derive(Debug, Default)]
pub struct XdpRouteMonitor {
    shared: SharedSnapshot,
    /// Monotonic generation stamped onto each published snapshot. Sufficient
    /// for the single-publisher model (one monitor thread); concurrent
    /// publishers still each get a distinct generation via `fetch_add`.
    next_generation: AtomicU64,
}

impl XdpRouteMonitor {
    /// Creates an empty monitor fanout.
    #[must_use]
    pub fn new() -> Self {
        Self {
            shared: Arc::new(ArcSwapOption::empty()),
            next_generation: AtomicU64::new(0),
        }
    }

    /// Registers a queue owner and returns its cold-path update handle.
    /// Registration is O(1) and the returned handle observes whatever
    /// snapshot is current at the time of the next `apply_updates` call.
    pub fn register_queue(&mut self) -> XdpRouteMonitorHandle {
        XdpRouteMonitorHandle {
            shared: Arc::clone(&self.shared),
            last_seen_generation: 0,
        }
    }

    /// Publishes a snapshot. Every registered subscriber sees this snapshot
    /// on its next `apply_updates` call. There is no per-subscriber queue:
    /// publishing a second snapshot before subscribers have observed the
    /// first is intentional — only the latest matters. Returns the number
    /// of currently-registered subscribers as an informational metric.
    pub fn publish(&self, snapshot: RouteSnapshot) -> usize {
        // Generations start at 1 so a handle's initial `last_seen_generation`
        // of 0 always reads as "new".
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed) + 1;
        self.shared.store(Some(Arc::new(VersionedSnapshot {
            generation,
            snapshot,
        })));
        Arc::strong_count(&self.shared).saturating_sub(1)
    }

    /// Starts a placeholder monitor thread. Real netlink polling is wired in a
    /// later pass; the fanout shape is fixed here so queues need only one
    /// monitor source.
    pub fn start(self, initial: RouteSnapshot) -> JoinHandle<Self> {
        thread::Builder::new()
            .name("fastsock-xdp-route-monitor".to_string())
            .spawn(move || {
                let _ = self.publish(initial);
                self
            })
            .expect("route monitor thread starts")
    }

    /// Starts one netlink snapshot-refresh thread and fans each snapshot out to
    /// all registered queues.
    ///
    /// Queue workers adopt snapshots through [`XdpRouteMonitorHandle`] outside
    /// their packet path; the route read path remains queue-local immutable
    /// memory with no cross-core synchronization instruction.
    ///
    /// Netlink errors are logged to stderr with exponential backoff so a
    /// persistent failure (permission denied, netlink socket exhausted, etc.)
    /// is visible to the operator instead of being silently retried.
    pub fn start_netlink(self, queue: QueueId, interval: Duration) -> JoinHandle<()> {
        thread::Builder::new()
            .name("fastsock-xdp-route-monitor".to_string())
            .spawn(move || {
                let mut consecutive_failures: u32 = 0;
                loop {
                    match RouteSnapshot::from_netlink() {
                        Ok(snapshot) => {
                            consecutive_failures = 0;
                            let _ = self.publish(snapshot);
                        }
                        Err(error) => {
                            // Log the first failure immediately, then only when
                            // the running failure count reaches a power of two
                            // (2nd, 3rd, 5th, 9th, 17th, ... failure) so a stuck
                            // monitor keeps a presence in stderr without
                            // flooding it.
                            if consecutive_failures
                                .is_power_of_two()
                                || consecutive_failures == 0
                            {
                                eprintln!(
                                    "fastsock-xdp-route-monitor: netlink refresh for queue {} failed ({} consecutive): {error}",
                                    queue.get(),
                                    consecutive_failures.saturating_add(1),
                                );
                            }
                            consecutive_failures = consecutive_failures.saturating_add(1);
                        }
                    }
                    thread::sleep(interval);
                }
            })
            .expect("route monitor thread starts")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_monitor_fans_out_to_many_queues() {
        let mut monitor = XdpRouteMonitor::new();
        let mut left = monitor.register_queue();
        let mut right = monitor.register_queue();
        monitor.publish(RouteSnapshot::new());

        let mut left_routes = XdpLocalRoutes::default();
        let mut right_routes = XdpLocalRoutes::default();
        assert_eq!(left.apply_updates(&mut left_routes), 1);
        assert_eq!(right.apply_updates(&mut right_routes), 1);

        // A second `apply_updates` with no new publish is a no-op.
        assert_eq!(left.apply_updates(&mut left_routes), 0);
        assert_eq!(right.apply_updates(&mut right_routes), 0);

        // Publishing a new snapshot makes both subscribers re-apply.
        monitor.publish(RouteSnapshot::new());
        assert_eq!(left.apply_updates(&mut left_routes), 1);
        assert_eq!(right.apply_updates(&mut right_routes), 1);
    }
}
