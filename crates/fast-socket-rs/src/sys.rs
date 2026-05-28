//! Shared system-level vocabulary used by backend builders and socket traits.

/// Operating-system interface index.
///
/// On Linux (and other Unixes) the kernel reserves index 0 as the "no
/// interface" sentinel returned by `if_nametoindex` on error. Constructing an
/// `IfIndex` from 0 is therefore forbidden so the type can be a strong
/// guarantee that the value identifies a real interface.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct IfIndex(u32);

impl IfIndex {
    /// Creates an interface index from its raw numeric value.
    ///
    /// Panics if `value == 0`. Use [`IfIndex::try_new`] when the value comes
    /// from an untrusted source that may legitimately produce zero.
    #[must_use]
    pub const fn new(value: u32) -> Self {
        assert!(
            value != 0,
            "IfIndex::new requires a non-zero interface index",
        );
        Self(value)
    }

    /// Creates an interface index, returning `None` for the reserved zero.
    #[must_use]
    pub const fn try_new(value: u32) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
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
    ///
    /// In practice every backend that consumes this enum treats `Default` as
    /// "try hugepages first, fall back to regular pages" — see
    /// [`Self::Size4K`] for the explicit no-hugepage path.
    #[default]
    Default,
    /// Skip hugepages entirely and use regular 4 KiB pages. Useful in test or
    /// CI environments where the system has no `nr_hugepages` configured and
    /// the implicit fallback adds noticeable open-time latency.
    Size4K,
    /// Prefer 2 MiB hugepages.
    Size2M,
    /// Prefer 1 GiB hugepages.
    Size1G,
}
