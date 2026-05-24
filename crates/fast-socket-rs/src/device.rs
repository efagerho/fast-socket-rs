//! Optional device and port side API for raw backends.

use crate::{Error, IfIndex, NumaNode, QueueAffinity, QueueId};

/// Raw-device capability flags.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct Capabilities(u64);

impl Capabilities {
    /// No capabilities.
    pub const NONE: Self = Self(0);
    /// IPv4 checksum offload.
    pub const CHECKSUM_IPV4: Self = Self(1 << 0);
    /// L4 checksum offload.
    pub const CHECKSUM_L4: Self = Self(1 << 1);
    /// Receive-side scaling.
    pub const RSS: Self = Self(1 << 2);
    /// Hardware transmit segmentation offload.
    pub const TSO: Self = Self(1 << 3);
    /// Receive coalescing.
    pub const GRO: Self = Self(1 << 4);
    /// Tunnel-aware RSS.
    pub const TUNNEL_RSS: Self = Self(1 << 5);
    /// Receive timestamping.
    pub const RX_TIMESTAMP: Self = Self(1 << 6);
    /// Transmit timestamping.
    pub const TX_TIMESTAMP: Self = Self(1 << 7);
    /// Inline security features.
    pub const INLINE_SECURITY: Self = Self(1 << 8);

    /// Creates capabilities from raw bits.
    #[must_use]
    pub const fn from_bits(bits: u64) -> Self {
        Self(bits)
    }

    /// Returns the raw bit representation.
    #[must_use]
    pub const fn bits(self) -> u64 {
        self.0
    }

    /// Returns `true` when all `capabilities` are present.
    #[must_use]
    pub const fn contains(self, capabilities: Self) -> bool {
        (self.0 & capabilities.0) == capabilities.0
    }

    /// Returns these capabilities plus `capabilities`.
    #[must_use]
    pub const fn union(self, capabilities: Self) -> Self {
        Self(self.0 | capabilities.0)
    }
}

impl core::ops::BitOr for Capabilities {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        self.union(rhs)
    }
}

impl core::ops::BitOrAssign for Capabilities {
    fn bitor_assign(&mut self, rhs: Self) {
        *self = self.union(rhs);
    }
}

/// Cumulative raw device or queue statistics snapshot.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RawDeviceStats {
    /// Successfully received packets.
    pub rx_packets: u64,
    /// Successfully received bytes.
    pub rx_bytes: u64,
    /// Successfully transmitted packets.
    pub tx_packets: u64,
    /// Successfully transmitted bytes.
    pub tx_bytes: u64,
    /// Dropped fragmented packets when the backend filters them.
    pub dropped_fragments: u64,
    /// Transmit attempts that exceeded MTU.
    pub dropped_oversize: u64,
    /// Ring-full events or retries.
    pub ring_full: u64,
}

/// Optional device and port side API for raw backends.
pub trait RawDevice {
    /// Stable identity for the underlying NIC port.
    fn ifindex(&self) -> IfIndex;

    /// Static capability bitset for this port.
    fn capabilities(&self) -> Capabilities;

    /// Per-queue affinity hint.
    fn queue_affinity(&self, queue: QueueId) -> QueueAffinity;

    /// NUMA node of the queue's DMA-visible memory.
    fn queue_numa_node(&self, queue: QueueId) -> Option<NumaNode>;

    /// Snapshot of cumulative counters for this device or queue.
    fn stats(&self, queue: QueueId) -> RawDeviceStats;

    /// Re-reads the device MTU on administrative change.
    fn refresh_mtu(&mut self) -> Result<u32, Error>;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockDevice {
        mtu: u32,
    }

    impl RawDevice for MockDevice {
        fn ifindex(&self) -> IfIndex {
            IfIndex::new(3)
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities::RSS | Capabilities::CHECKSUM_L4
        }

        fn queue_affinity(&self, _queue: QueueId) -> QueueAffinity {
            QueueAffinity::Core(5)
        }

        fn queue_numa_node(&self, _queue: QueueId) -> Option<NumaNode> {
            Some(NumaNode::new(0))
        }

        fn stats(&self, _queue: QueueId) -> RawDeviceStats {
            RawDeviceStats {
                rx_packets: 10,
                ..RawDeviceStats::default()
            }
        }

        fn refresh_mtu(&mut self) -> Result<u32, Error> {
            Ok(self.mtu)
        }
    }

    #[test]
    fn raw_device_side_api_exposes_capabilities_and_stats() {
        let mut device = MockDevice { mtu: 1500 };

        assert_eq!(device.ifindex(), IfIndex::new(3));
        assert!(device.capabilities().contains(Capabilities::RSS));
        assert_eq!(
            device.queue_affinity(QueueId::new(0)),
            QueueAffinity::Core(5)
        );
        assert_eq!(device.stats(QueueId::new(0)).rx_packets, 10);
        assert_eq!(device.refresh_mtu().unwrap(), 1500);
    }
}
