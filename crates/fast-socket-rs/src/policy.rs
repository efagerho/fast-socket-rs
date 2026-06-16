//! Polling policy traits and canonical driver types.

use core::time::Duration;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use crate::Error;

#[cfg(not(unix))]
use core::marker::PhantomData;

#[cfg(unix)]
use std::os::fd::{AsFd, BorrowedFd};

/// Socket polling regime selected by a concrete driver type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PollMode {
    /// Wait-driven polling through an external event source.
    WaitDriven,
    /// Busy-polling on a dedicated worker core.
    BusyPoll,
}

/// Outcome of a polling wait operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WaitOutcome {
    /// At least one receive, transmit-completion, or wake source has work.
    Ready,
    /// The timeout elapsed with no work.
    Timeout,
    /// Returned without work; callers should continue their worker loop.
    Spurious,
}

/// Borrowed wake handle for Unix platforms.
#[cfg(unix)]
#[derive(Clone, Copy, Debug)]
pub struct WakeHandle<'a> {
    fd: BorrowedFd<'a>,
}

#[cfg(unix)]
impl<'a> WakeHandle<'a> {
    /// Creates a wake handle from a borrowed file descriptor.
    #[must_use]
    pub const fn from_fd(fd: BorrowedFd<'a>) -> Self {
        Self { fd }
    }

    /// Returns the borrowed file descriptor.
    #[must_use]
    pub const fn borrowed_fd(self) -> BorrowedFd<'a> {
        self.fd
    }
}

#[cfg(unix)]
impl AsFd for WakeHandle<'_> {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd
    }
}

/// Borrowed wake token for non-Unix platforms.
#[cfg(not(unix))]
#[derive(Clone, Copy, Debug)]
pub struct WakeHandle<'a> {
    token: usize,
    _marker: PhantomData<&'a ()>,
}

#[cfg(not(unix))]
impl<'a> WakeHandle<'a> {
    /// Creates a wake handle from an opaque borrowed token.
    #[must_use]
    pub const fn from_token(token: usize) -> Self {
        Self {
            token,
            _marker: PhantomData,
        }
    }

    /// Returns the opaque borrowed token.
    #[must_use]
    pub const fn token(self) -> usize {
        self.token
    }
}

/// Companion trait used by sockets to expose their polling behavior.
pub trait PollDriver {
    /// Compile-time polling mode selected by this driver.
    const MODE: PollMode;

    /// Waits for work, timeout, or a spurious wakeup.
    fn wait(&mut self, timeout: Option<Duration>) -> Result<WaitOutcome, Error>;

    /// Returns a borrowed wake handle when this driver supports one.
    fn wake_handle(&self) -> Option<WakeHandle<'_>>;
}

/// Marker trait for drivers that are intended to be polled continuously.
pub trait BusyPollDriverKind: PollDriver {}

/// Marker trait for drivers that can wait on an external event source.
pub trait WaitDrivenDriverKind: PollDriver {}

/// Busy-poll driver for sockets that own a worker core.
#[derive(Clone, Copy, Debug, Default)]
pub struct BusyPollDriver;

impl BusyPollDriver {
    /// Creates a busy-poll driver.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl PollDriver for BusyPollDriver {
    const MODE: PollMode = PollMode::BusyPoll;

    #[inline(always)]
    fn wait(&mut self, _timeout: Option<Duration>) -> Result<WaitOutcome, Error> {
        Ok(WaitOutcome::Spurious)
    }

    #[inline(always)]
    fn wake_handle(&self) -> Option<WakeHandle<'_>> {
        None
    }
}

impl BusyPollDriverKind for BusyPollDriver {}

/// Type-level IP family policy used by IP packet sockets and packet policies.
pub trait IpFamily {
    /// Address type used by this family policy.
    type Addr: Copy + Eq;
}

/// Mixed IPv4/IPv6 family policy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Mixed;

impl IpFamily for Mixed {
    type Addr = IpAddr;
}

/// IPv4-only family policy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct V4Only;

impl IpFamily for V4Only {
    type Addr = Ipv4Addr;
}

/// IPv6-only family policy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct V6Only;

impl IpFamily for V6Only {
    type Addr = Ipv6Addr;
}
