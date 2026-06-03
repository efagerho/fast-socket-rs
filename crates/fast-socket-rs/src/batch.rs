//! Batch containers and ownership-transfer helpers.

use core::fmt;

use crate::Error;

/// A transmit slot that can be consumed in-place by a socket implementation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TxSlot<T> {
    /// The slot contains a packet ready to transmit.
    Ready(T),
    /// The slot was accepted by a socket and its packet ownership was taken.
    Taken,
}

impl<T> TxSlot<T> {
    /// Returns `true` when the slot contains a ready item.
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        matches!(self, Self::Ready(_))
    }

    /// Returns `true` when the slot has already been consumed.
    #[must_use]
    pub const fn is_taken(&self) -> bool {
        matches!(self, Self::Taken)
    }

    /// Borrows the ready item, if present.
    #[must_use]
    pub const fn as_ref(&self) -> Option<&T> {
        match self {
            Self::Ready(item) => Some(item),
            Self::Taken => None,
        }
    }

    /// Mutably borrows the ready item, if present.
    #[must_use]
    pub fn as_mut(&mut self) -> Option<&mut T> {
        match self {
            Self::Ready(item) => Some(item),
            Self::Taken => None,
        }
    }

    /// Takes ownership of the ready item and leaves the slot marked as taken.
    pub fn take(&mut self) -> Option<T> {
        match core::mem::replace(self, Self::Taken) {
            Self::Ready(item) => Some(item),
            Self::Taken => None,
        }
    }
}

impl<T> From<T> for TxSlot<T> {
    fn from(value: T) -> Self {
        Self::Ready(value)
    }
}

/// Error returned by a batch send after accepting a prefix of the submitted batch.
#[derive(Clone, Debug)]
pub struct SendError {
    /// Number of leading slots accepted and consumed before the error.
    ///
    /// Implementations must have set slots `[0..accepted)` to
    /// [`TxSlot::Taken`] before returning this error. Slots at index
    /// `>= accepted` are left in their caller-provided state and may still hold
    /// [`TxSlot::Ready`] items.
    pub accepted: usize,
    /// Error that caused the next slot to be rejected.
    pub kind: Error,
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "send failed after accepting {} items: {}",
            self.accepted, self.kind
        )
    }
}

impl std::error::Error for SendError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.kind.source()
    }
}

/// Reusable receive batch with caller-controlled capacity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecvBatch<T> {
    items: Vec<T>,
    capacity: usize,
}

impl<T> RecvBatch<T> {
    /// Creates an empty receive batch with room for at most `capacity` items.
    ///
    /// Panics if `capacity == 0`. A zero-capacity batch can never accept a
    /// packet, so every `recv` call would either return `Ok(0)` immediately
    /// (without actually polling the socket) or surface [`Error::BatchFull`]
    /// (`crate::Error::BatchFull`) — neither matches what a caller usually
    /// intends.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(
            capacity > 0,
            "RecvBatch::with_capacity requires capacity >= 1",
        );
        Self {
            items: Vec::with_capacity(capacity),
            capacity,
        }
    }

    /// Returns the maximum number of items this batch accepts.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the number of received items currently stored in the batch.
    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Returns `true` when the batch contains no items.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Returns the number of items that can still be pushed before the batch is
    /// full (its fixed `capacity` minus its current `len`).
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.capacity.saturating_sub(self.items.len())
    }

    /// Clears the current items while preserving the allocated storage.
    pub fn clear(&mut self) {
        self.items.clear();
    }

    /// Pushes one received item if capacity remains.
    ///
    /// Returns the item back to the caller when the batch is full.
    pub fn push(&mut self, item: T) -> core::result::Result<(), T> {
        if self.items.len() >= self.capacity {
            Err(item)
        } else {
            self.items.push(item);
            Ok(())
        }
    }

    /// Returns the received items as a slice.
    #[must_use]
    pub fn as_slice(&self) -> &[T] {
        &self.items
    }

    /// Returns the received items as a mutable slice.
    #[must_use]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.items
    }

    /// Drains all received items from the batch.
    pub fn drain(&mut self) -> impl Iterator<Item = T> + '_ {
        self.items.drain(..)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn send_with_rejection(batch: &mut [TxSlot<u32>]) -> Result<usize, SendError> {
        let mut accepted = 0;
        for slot in batch.iter_mut() {
            match slot.as_ref() {
                Some(value) if *value == 99 => {
                    return Err(SendError {
                        accepted,
                        kind: Error::OversizeForMtu,
                    });
                }
                Some(_) => {
                    let _ = slot.take();
                    accepted += 1;
                }
                None => {
                    return Err(SendError {
                        accepted,
                        kind: Error::InvalidBatch,
                    });
                }
            }
        }
        Ok(accepted)
    }

    #[test]
    fn send_error_leaves_rejected_slot_and_tail_untouched() {
        let mut batch = [
            TxSlot::Ready(1u32),
            TxSlot::Ready(2),
            TxSlot::Ready(99),
            TxSlot::Ready(4),
        ];

        let err = send_with_rejection(&mut batch).expect_err("third slot must be rejected");
        assert_eq!(err.accepted, 2);
        assert!(matches!(err.kind, Error::OversizeForMtu));

        assert!(batch[0].is_taken());
        assert!(batch[1].is_taken());
        assert_eq!(batch[2].as_ref(), Some(&99));
        assert_eq!(batch[3].as_ref(), Some(&4));
    }

    #[test]
    fn send_short_accept_leaves_tail_untouched() {
        let mut batch = [TxSlot::Ready(1u32), TxSlot::Ready(2), TxSlot::Ready(3)];
        // Simulate a partial accept: only the first item is taken.
        let _ = batch[0].take();
        let accepted = 1;

        assert!(batch[0].is_taken());
        assert_eq!(batch[1].as_ref(), Some(&2));
        assert_eq!(batch[2].as_ref(), Some(&3));
        assert_eq!(accepted, 1);
    }
}
