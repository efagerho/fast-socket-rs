//! Routing identifiers.
use core::fmt;

/// Opaque route identifier for core egress handles and backend route state.
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
