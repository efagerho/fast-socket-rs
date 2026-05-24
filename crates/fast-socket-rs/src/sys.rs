//! Shared system-level vocabulary used by backend builders and socket traits.

/// Operating-system interface index.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct IfIndex(u32);

impl IfIndex {
    /// Creates an interface index from its raw numeric value.
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns the raw numeric interface index.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Queue identifier within a port, interface, or backend device.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct QueueId(u32);

impl QueueId {
    /// Creates a queue identifier from its raw numeric value.
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns the raw numeric queue identifier.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// NUMA node identifier.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct NumaNode(u16);

impl NumaNode {
    /// Creates a NUMA node identifier from its raw numeric value.
    #[must_use]
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    /// Returns the raw numeric NUMA node identifier.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }
}

/// CPU affinity hint for a queue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum QueueAffinity {
    /// No preferred CPU is known.
    Any,
    /// A single preferred CPU core.
    Core(u32),
    /// A compact CPU mask for small systems.
    Mask(u64),
}

/// Hugepage size preference for backends that allocate hugepage-backed memory.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum HugePageSize {
    /// Use the backend or operating-system default hugepage size.
    #[default]
    Default,
    /// Prefer 2 MiB hugepages.
    Size2M,
    /// Prefer 1 GiB hugepages.
    Size1G,
}
