//! Route, neighbor, and tunnel identifiers.

/// Opaque route identifier.
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

/// Opaque neighbor identifier.
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

/// Opaque tunnel identifier.
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
