//! Single route-monitor fanout for XDP queues.

use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use fast_socket_rs::QueueId;

use crate::route::{RouteSnapshot, XdpLocalRoutes};

/// Handle used by queue owners to receive cold-path route updates.
#[derive(Debug)]
pub struct XdpRouteMonitorHandle {
    receiver: Receiver<RouteSnapshot>,
}

impl XdpRouteMonitorHandle {
    /// Applies all queued snapshots to `routes`.
    pub fn apply_updates(&mut self, routes: &mut XdpLocalRoutes) -> usize {
        let mut applied = 0;
        while let Ok(snapshot) = self.receiver.try_recv() {
            routes.push_update(snapshot);
            applied += routes.apply_updates();
        }
        applied
    }
}

/// One route monitor fanout serving multiple queue owners.
#[derive(Debug)]
pub struct XdpRouteMonitor {
    subscribers: Vec<Sender<RouteSnapshot>>,
}

impl XdpRouteMonitor {
    /// Creates an empty monitor fanout.
    #[must_use]
    pub fn new() -> Self {
        Self {
            subscribers: Vec::new(),
        }
    }

    /// Registers a queue owner and returns its cold-path update handle.
    pub fn register_queue(&mut self) -> XdpRouteMonitorHandle {
        let (sender, receiver) = mpsc::channel();
        self.subscribers.push(sender);
        XdpRouteMonitorHandle { receiver }
    }

    /// Publishes a snapshot to all registered queues.
    pub fn publish(&mut self, snapshot: RouteSnapshot) -> usize {
        let mut delivered = 0;
        self.subscribers.retain(|sender| {
            if sender.send(snapshot.clone()).is_ok() {
                delivered += 1;
                true
            } else {
                false
            }
        });
        delivered
    }

    /// Starts a placeholder monitor thread. Real netlink polling is wired in a
    /// later pass; the fanout shape is fixed here so queues need only one
    /// monitor source.
    pub fn start(mut self, initial: RouteSnapshot) -> JoinHandle<Self> {
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
    pub fn start_netlink(mut self, queue: QueueId, interval: Duration) -> JoinHandle<()> {
        thread::Builder::new()
            .name("fastsock-xdp-route-monitor".to_string())
            .spawn(move || {
                loop {
                    if let Ok(snapshot) = RouteSnapshot::from_netlink(queue) {
                        let _ = self.publish(snapshot);
                    }
                    thread::sleep(interval);
                }
            })
            .expect("route monitor thread starts")
    }
}

impl Default for XdpRouteMonitor {
    fn default() -> Self {
        Self::new()
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
        assert_eq!(monitor.publish(RouteSnapshot::new()), 2);

        let mut left_routes = XdpLocalRoutes::default();
        let mut right_routes = XdpLocalRoutes::default();
        assert_eq!(left.apply_updates(&mut left_routes), 1);
        assert_eq!(right.apply_updates(&mut right_routes), 1);
    }
}
