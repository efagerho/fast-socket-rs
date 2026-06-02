//! Shared error vocabulary for core operations.

use core::fmt;
use std::sync::Arc;

/// Result alias for operations that use the core [`Error`] type.
pub type Result<T> = core::result::Result<T, Error>;

/// Common error type shared by core APIs.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Transient: the operation cannot make progress immediately.
    ///
    /// Caller-side capacity failures use [`Error::BatchFull`] instead so
    /// callers can disambiguate the two.
    WouldBlock,
    /// Receive or transmit path: packet bytes or metadata were malformed.
    InvalidPacket,
    /// Caller submitted a malformed batch.
    InvalidBatch,
    /// Backend-reported device or descriptor failure.
    Device(DeviceError),
    /// Receive path: caller-provided output storage has no remaining capacity.
    ///
    /// Distinct from [`Error::WouldBlock`], which indicates transient
    /// back-pressure. `BatchFull` means the **caller-supplied** container is
    /// full and at least one delivered packet was discarded for that reason.
    BatchFull,
    /// Receive path: an incoming packet did not fit in the caller-provided space.
    Truncated,
    /// Transmit path: packet exceeds the socket MTU.
    ///
    /// Raised when the finished packet is too large for the configured wire
    /// MTU, after any caller-side packet construction has already completed.
    OversizeForMtu,
    /// Transmit path: the resolved egress handle no longer maps to a destination.
    NoEgressRoute,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WouldBlock => f.write_str("operation would block"),
            Self::InvalidPacket => f.write_str("packet was invalid"),
            Self::InvalidBatch => f.write_str("batch was invalid"),
            Self::Device(error) => write!(f, "device error: {error}"),
            Self::BatchFull => f.write_str("receive batch is full"),
            Self::Truncated => f.write_str("packet was truncated"),
            Self::OversizeForMtu => f.write_str("packet exceeds MTU"),
            Self::NoEgressRoute => f.write_str("no usable egress route"),
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

/// Core-owned device error container.
///
/// The optional backend-specific source is held behind an [`Arc`] so the whole
/// `DeviceError` (and the enclosing [`Error`]) can be cheaply cloned without
/// losing the source chain.
#[derive(Clone, Debug)]
pub struct DeviceError {
    kind: DeviceErrorKind,
    source: Option<Arc<dyn std::error::Error + Send + Sync + 'static>>,
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
            source: Some(Arc::new(source)),
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
