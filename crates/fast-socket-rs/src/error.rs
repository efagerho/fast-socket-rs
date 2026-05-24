//! Shared error vocabulary for the core socket traits.

use core::fmt;

/// Result alias for operations that use the core [`Error`] type.
pub type Result<T> = core::result::Result<T, Error>;

/// Common error type shared by UDP and IP packet socket APIs.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// Transient: the underlying ring or socket buffer is full.
    WouldBlock,
    /// Receive path: an incoming packet did not fit in the caller-provided space.
    Truncated,
    /// Receive or transmit path: packet bytes or metadata were malformed.
    InvalidPacket,
    /// Caller submitted a malformed batch.
    InvalidBatch,
    /// Transmit path: packet exceeds the socket MTU.
    OversizeForMtu,
    /// Transmit path: the resolved egress handle no longer maps to a destination.
    NoEgressRoute,
    /// Backend-reported device or descriptor failure.
    Device(DeviceError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WouldBlock => f.write_str("operation would block"),
            Self::Truncated => f.write_str("packet was truncated"),
            Self::InvalidPacket => f.write_str("packet was invalid"),
            Self::InvalidBatch => f.write_str("batch was invalid"),
            Self::OversizeForMtu => f.write_str("packet exceeds MTU"),
            Self::NoEgressRoute => f.write_str("no usable egress route"),
            Self::Device(error) => write!(f, "device error: {error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Device(error) => error.source(),
            _ => None,
        }
    }
}

/// Coarse category for a backend-reported device error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DeviceErrorKind {
    /// Backend-specific error not covered by a more specific variant.
    Backend,
    /// The underlying device disappeared or was administratively removed.
    DeviceRemoved,
    /// The underlying file descriptor or handle was closed.
    FdClosed,
    /// A descriptor ring or completion queue reported corruption.
    RingCorrupt,
}

impl fmt::Display for DeviceErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backend => f.write_str("backend error"),
            Self::DeviceRemoved => f.write_str("device removed"),
            Self::FdClosed => f.write_str("handle closed"),
            Self::RingCorrupt => f.write_str("ring corrupt"),
        }
    }
}

/// Core-owned, cold-path device error container.
#[derive(Debug)]
pub struct DeviceError {
    kind: DeviceErrorKind,
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

impl DeviceError {
    /// Creates a device error without a backend-specific source.
    #[must_use]
    pub const fn new(kind: DeviceErrorKind) -> Self {
        Self { kind, source: None }
    }

    /// Creates a device error with a backend-specific source attached.
    #[must_use]
    pub fn with_source<E>(kind: DeviceErrorKind, source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self {
            kind,
            source: Some(Box::new(source)),
        }
    }

    /// Returns the coarse device error category.
    #[must_use]
    pub const fn kind(&self) -> DeviceErrorKind {
        self.kind
    }
}

impl fmt::Display for DeviceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.source {
            Some(source) => write!(f, "{}: {source}", self.kind),
            None => self.kind.fmt(f),
        }
    }
}

impl std::error::Error for DeviceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}
