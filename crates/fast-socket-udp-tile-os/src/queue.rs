//! Bounded tile queues with compile-time wait strategies.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::{self, Thread};
use std::time::Duration;

use crossbeam_queue::ArrayQueue;

mod sealed {
    pub trait Sealed {}
}

/// Compile-time wait strategy for tile queues.
///
/// [`Spin`] compiles to pure polling. [`Park`] records the consumer thread and
/// lets producers unpark it after a successful push.
pub trait WaitStrategy: sealed::Sealed + Send + Sync + 'static {
    /// Per-queue state used by this strategy.
    type State: Default + Send + Sync + 'static;

    /// Called by a producer after a successful push.
    fn on_push(state: &Self::State);

    /// Called by the consumer thread before entering its worker loop.
    fn register_consumer(state: &Self::State);

    /// Marks the consumer as about to sleep.
    fn set_sleeping(state: &Self::State);

    /// Clears the sleeping flag after waking or after the empty re-check.
    fn clear_sleeping(state: &Self::State);

    /// Performs one bounded idle wait.
    fn do_wait();

    /// Orders `set_sleeping` before the empty re-check.
    fn fence_after_set_sleeping();
}

/// Busy-spin wait strategy.
#[derive(Clone, Copy, Debug, Default)]
pub struct Spin;

impl sealed::Sealed for Spin {}

impl WaitStrategy for Spin {
    type State = ();

    #[inline(always)]
    fn on_push(_: &()) {}

    #[inline(always)]
    fn register_consumer(_: &()) {}

    #[inline(always)]
    fn set_sleeping(_: &()) {}

    #[inline(always)]
    fn clear_sleeping(_: &()) {}

    #[inline(always)]
    fn do_wait() {
        std::hint::spin_loop();
    }

    #[inline(always)]
    fn fence_after_set_sleeping() {}
}

/// Park/unpark wait strategy.
#[derive(Clone, Copy, Debug, Default)]
pub struct Park;

impl sealed::Sealed for Park {}

/// Per-queue state used by [`Park`].
pub struct ParkState {
    sleeping: AtomicBool,
    consumer: OnceLock<Thread>,
}

impl Default for ParkState {
    fn default() -> Self {
        Self {
            sleeping: AtomicBool::new(false),
            consumer: OnceLock::new(),
        }
    }
}

impl WaitStrategy for Park {
    type State = ParkState;

    #[inline(always)]
    fn on_push(state: &ParkState) {
        if state.sleeping.load(Ordering::SeqCst)
            && let Some(thread) = state.consumer.get()
        {
            thread.unpark();
        }
    }

    #[inline(always)]
    fn register_consumer(state: &ParkState) {
        let _ = state.consumer.set(thread::current());
    }

    #[inline(always)]
    fn set_sleeping(state: &ParkState) {
        state.sleeping.store(true, Ordering::SeqCst);
    }

    #[inline(always)]
    fn clear_sleeping(state: &ParkState) {
        state.sleeping.store(false, Ordering::Relaxed);
    }

    #[inline(always)]
    fn do_wait() {
        thread::park_timeout(Duration::from_micros(50));
    }

    #[inline(always)]
    fn fence_after_set_sleeping() {
        std::sync::atomic::fence(Ordering::SeqCst);
    }
}

/// A bounded multi-producer/single-consumer queue used between tiles.
pub struct Queue<T, W: WaitStrategy> {
    inner: ArrayQueue<T>,
    state: W::State,
}

impl<T, W: WaitStrategy> Queue<T, W> {
    /// Creates a reference-counted bounded queue.
    #[must_use]
    pub fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: ArrayQueue::new(capacity.max(1)),
            state: W::State::default(),
        })
    }

    /// Pushes one item, returning it when the queue is full.
    #[inline]
    pub fn push(&self, item: T) -> Result<(), T> {
        self.inner.push(item)?;
        W::on_push(&self.state);
        Ok(())
    }

    /// Pops one item.
    #[inline]
    pub fn pop(&self) -> Option<T> {
        self.inner.pop()
    }

    /// Returns `true` if the queue is empty.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns the number of queued items.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns the queue capacity.
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Registers the calling thread as the consumer.
    #[inline]
    pub fn register_consumer(&self) {
        W::register_consumer(&self.state);
    }

    /// Runs the configured wait strategy if the queue remains empty.
    #[inline]
    pub fn wait_if_empty(&self) {
        W::set_sleeping(&self.state);
        W::fence_after_set_sleeping();
        if self.is_empty() {
            W::do_wait();
        }
        W::clear_sleeping(&self.state);
    }
}

/// Waits until at least one queue is non-empty, or until the strategy's bounded
/// idle wait returns.
pub fn wait_any_non_empty<T, W: WaitStrategy>(queues: &[Arc<Queue<T, W>>]) {
    for queue in queues {
        W::set_sleeping(&queue.state);
    }
    W::fence_after_set_sleeping();
    if queues.iter().all(|queue| queue.is_empty()) {
        W::do_wait();
    }
    for queue in queues {
        W::clear_sleeping(&queue.state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_returns_item_when_full() {
        let queue = Queue::<u32, Spin>::new(1);
        assert_eq!(queue.push(1), Ok(()));
        assert_eq!(queue.push(2), Err(2));
        assert_eq!(queue.pop(), Some(1));
    }
}
