//! Bounded tile queues with compile-time wait strategies.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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

    /// Registers the calling thread as the consumer.
    #[inline]
    pub fn register_consumer(&self) {
        W::register_consumer(&self.state);
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

/// Creates a bounded single-producer/single-consumer queue endpoint pair.
#[must_use]
pub fn spsc_pair<T>(capacity: usize) -> (SpscProducer<T>, SpscConsumer<T>) {
    let queue = Arc::new(SpscQueue::new(capacity));
    (
        SpscProducer {
            queue: Arc::clone(&queue),
        },
        SpscConsumer { queue },
    )
}

/// Producer endpoint for a bounded single-producer/single-consumer queue.
pub struct SpscProducer<T> {
    queue: Arc<SpscQueue<T>>,
}

impl<T> SpscProducer<T> {
    /// Pushes one item, returning it when the queue is full.
    #[inline]
    pub fn push(&mut self, item: T) -> Result<(), T> {
        self.queue.push(item)
    }

    /// Returns the number of queued items.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Returns the number of items that can be pushed without filling the queue.
    #[inline]
    #[must_use]
    pub fn remaining_capacity(&self) -> usize {
        self.queue.remaining_capacity()
    }
}

/// Consumer endpoint for a bounded single-producer/single-consumer queue.
pub struct SpscConsumer<T> {
    queue: Arc<SpscQueue<T>>,
}

impl<T> SpscConsumer<T> {
    /// Pops one item.
    #[inline]
    #[cfg(test)]
    pub fn pop(&mut self) -> Option<T> {
        self.queue.pop()
    }

    /// Pops up to `count` items directly into `out`.
    #[inline]
    pub fn pop_into(&mut self, count: usize, out: &mut Vec<T>) -> usize {
        self.queue.pop_into(count, out)
    }
}

struct SpscQueue<T> {
    buffer: Box<[UnsafeCell<MaybeUninit<T>>]>,
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
}

#[repr(align(64))]
struct CachePadded<T>(T);

impl<T> SpscQueue<T> {
    fn new(capacity: usize) -> Self {
        let slots = capacity.max(1).saturating_add(1);
        let mut buffer = Vec::with_capacity(slots);
        for _ in 0..slots {
            buffer.push(UnsafeCell::new(MaybeUninit::uninit()));
        }
        Self {
            buffer: buffer.into_boxed_slice(),
            head: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(0)),
        }
    }

    #[inline]
    fn push(&self, item: T) -> Result<(), T> {
        let tail = self.tail.0.load(Ordering::Relaxed);
        let next_tail = self.next(tail);
        if next_tail == self.head.0.load(Ordering::Acquire) {
            return Err(item);
        }

        unsafe {
            (*self.buffer[tail].get()).write(item);
        }
        self.tail.0.store(next_tail, Ordering::Release);
        Ok(())
    }

    #[inline]
    #[cfg(test)]
    fn pop(&self) -> Option<T> {
        let head = self.head.0.load(Ordering::Relaxed);
        if head == self.tail.0.load(Ordering::Acquire) {
            return None;
        }

        let item = unsafe { (*self.buffer[head].get()).assume_init_read() };
        self.head.0.store(self.next(head), Ordering::Release);
        Some(item)
    }

    #[inline]
    fn pop_into(&self, count: usize, out: &mut Vec<T>) -> usize {
        if count == 0 {
            return 0;
        }

        out.reserve(count);
        let base_len = out.len();
        let mut written = 0usize;
        let mut head = self.head.0.load(Ordering::Relaxed);
        let mut tail = self.tail.0.load(Ordering::Acquire);

        while written < count {
            if head == tail {
                tail = self.tail.0.load(Ordering::Acquire);
                if head == tail {
                    break;
                }
            }

            let item = unsafe { (*self.buffer[head].get()).assume_init_read() };
            unsafe {
                out.as_mut_ptr().add(base_len + written).write(item);
            }
            head = self.next(head);
            written += 1;
        }

        if written != 0 {
            self.head.0.store(head, Ordering::Release);
            unsafe {
                out.set_len(base_len + written);
            }
        }
        written
    }

    #[inline]
    fn len(&self) -> usize {
        let head = self.head.0.load(Ordering::Acquire);
        let tail = self.tail.0.load(Ordering::Acquire);
        if tail >= head {
            tail - head
        } else {
            self.slots() - head + tail
        }
    }

    #[inline]
    fn capacity(&self) -> usize {
        self.slots() - 1
    }

    #[inline]
    fn remaining_capacity(&self) -> usize {
        self.capacity().saturating_sub(self.len())
    }

    #[inline]
    fn next(&self, index: usize) -> usize {
        let next = index + 1;
        if next == self.slots() { 0 } else { next }
    }

    #[inline]
    fn slots(&self) -> usize {
        self.buffer.len()
    }
}

unsafe impl<T: Send> Send for SpscQueue<T> {}
unsafe impl<T: Send> Sync for SpscQueue<T> {}

impl<T> Drop for SpscQueue<T> {
    fn drop(&mut self) {
        let mut head = self.head.0.load(Ordering::Relaxed);
        let tail = self.tail.0.load(Ordering::Relaxed);
        while head != tail {
            unsafe {
                self.buffer[head].get_mut().assume_init_drop();
            }
            head = self.next(head);
        }
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

    #[test]
    fn spsc_queue_returns_item_when_full() {
        let (mut producer, mut consumer) = spsc_pair(1);
        assert_eq!(producer.push(1), Ok(()));
        assert_eq!(producer.push(2), Err(2));
        assert_eq!(consumer.pop(), Some(1));
        assert_eq!(consumer.pop(), None);
    }

    #[test]
    fn spsc_queue_pops_many_into_vec() {
        let (mut producer, mut consumer) = spsc_pair(4);
        assert_eq!(producer.push(1), Ok(()));
        assert_eq!(producer.push(2), Ok(()));
        let mut out = vec![0];
        assert_eq!(consumer.pop_into(4, &mut out), 2);
        assert_eq!(out, [0, 1, 2]);
        assert_eq!(consumer.pop(), None);
    }
}
