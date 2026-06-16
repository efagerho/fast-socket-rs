//! Bounded tile queues with compile-time tile polling modes.

use std::cell::UnsafeCell;
use std::io;
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_queue::ArrayQueue;
use fast_socket_rs::{BusyPollDriverKind, PollDriver, WaitDrivenDriverKind};

mod sealed {
    pub trait Sealed {}
}

/// Tile worker polling mode.
///
/// [`Spin`] is for busy-poll sockets. [`Park`] is for wait-driven sockets and
/// sleeps on socket wake handles plus per-lane transmit wake handles.
pub trait TilePollMode: sealed::Sealed + Send + Sync + 'static {
    /// Per-lane wake state used by this mode.
    type State: Default + Send + Sync + 'static;

    /// Concrete polling mode kind.
    const KIND: TilePollModeKind;

    /// Called by a producer after a successful push.
    fn on_push(state: &Self::State);

    /// Returns the fd that wakes a parked tile worker for this lane.
    fn wake_fd(state: &Self::State) -> Option<RawFd>;
}

/// Concrete tile polling mode kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TilePollModeKind {
    /// Busy-polling tile loop.
    Spin,
    /// Wait-driven tile loop.
    Park,
}

/// Marker trait implemented when a socket driver supports a tile polling mode.
pub trait TilePollModeDriver<M: TilePollMode>: PollDriver {}

impl<D> TilePollModeDriver<Spin> for D where D: BusyPollDriverKind {}

impl<D> TilePollModeDriver<Park> for D where D: WaitDrivenDriverKind {}

/// Busy-poll tile mode.
#[derive(Clone, Copy, Debug, Default)]
pub struct Spin;

impl sealed::Sealed for Spin {}

impl TilePollMode for Spin {
    type State = ();

    const KIND: TilePollModeKind = TilePollModeKind::Spin;

    #[inline(always)]
    fn on_push(_: &()) {}

    #[inline(always)]
    fn wake_fd(_: &()) -> Option<RawFd> {
        None
    }
}

/// Wait-driven tile mode.
#[derive(Clone, Copy, Debug, Default)]
pub struct Park;

impl sealed::Sealed for Park {}

/// Per-lane wake state used by [`Park`].
pub struct ParkState {
    fd: OwnedFd,
}

impl Default for ParkState {
    fn default() -> Self {
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if fd < 0 {
            panic!(
                "failed to create UDP tile eventfd wake handle: {}",
                io::Error::last_os_error()
            );
        }
        Self {
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
        }
    }
}

impl TilePollMode for Park {
    type State = ParkState;

    const KIND: TilePollModeKind = TilePollModeKind::Park;

    #[inline(always)]
    fn on_push(state: &ParkState) {
        let value = 1u64;
        let rc = unsafe {
            libc::write(
                state.fd.as_raw_fd(),
                std::ptr::addr_of!(value).cast(),
                std::mem::size_of::<u64>(),
            )
        };
        if rc < 0 {
            let error = io::Error::last_os_error();
            if !matches!(error.raw_os_error(), Some(libc::EAGAIN)) {
                debug_assert!(false, "failed to signal UDP tile eventfd: {error}");
            }
        }
    }

    #[inline(always)]
    fn wake_fd(state: &ParkState) -> Option<RawFd> {
        Some(state.fd.as_raw_fd())
    }
}

pub(crate) struct Queue<T> {
    inner: ArrayQueue<T>,
}

impl<T> Queue<T> {
    #[must_use]
    pub(crate) fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: ArrayQueue::new(capacity.max(1)),
        })
    }

    #[inline]
    pub(crate) fn push(&self, item: T) -> Result<(), T> {
        self.inner.push(item)
    }

    #[inline]
    pub(crate) fn pop(&self) -> Option<T> {
        self.inner.pop()
    }
}

pub(crate) struct Wake<W: TilePollMode> {
    state: W::State,
}

impl<W: TilePollMode> Wake<W> {
    #[must_use]
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: W::State::default(),
        })
    }

    #[inline]
    pub(crate) fn notify(&self) {
        W::on_push(&self.state);
    }

    #[inline]
    pub(crate) fn wake_fd(&self) -> Option<RawFd> {
        W::wake_fd(&self.state)
    }
}

#[must_use]
pub(crate) fn spsc_pair<T>(capacity: usize) -> (SpscProducer<T>, SpscConsumer<T>) {
    let queue = Arc::new(SpscQueue::new(capacity));
    (
        SpscProducer {
            queue: Arc::clone(&queue),
        },
        SpscConsumer { queue },
    )
}

pub(crate) struct SpscProducer<T> {
    queue: Arc<SpscQueue<T>>,
}

impl<T> SpscProducer<T> {
    #[inline]
    pub(crate) fn push(&mut self, item: T) -> Result<(), T> {
        self.queue.push(item)
    }

    #[inline]
    pub(crate) unsafe fn push_many_from<U, F>(&mut self, source: &mut Vec<U>, map: F) -> usize
    where
        F: FnMut(U) -> T,
    {
        unsafe { self.queue.push_many_from(source, map) }
    }

    #[inline]
    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.queue.len()
    }

    #[inline]
    #[must_use]
    pub(crate) fn remaining_capacity(&self) -> usize {
        self.queue.remaining_capacity()
    }
}

pub(crate) struct SpscConsumer<T> {
    queue: Arc<SpscQueue<T>>,
}

impl<T> SpscConsumer<T> {
    #[inline]
    pub(crate) fn pop(&mut self) -> Option<T> {
        self.queue.pop()
    }

    #[inline]
    pub(crate) fn pop_into(&mut self, count: usize, out: &mut Vec<T>) -> usize {
        self.queue.pop_into(count, out)
    }

    #[inline]
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.queue.is_empty()
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
    unsafe fn push_many_from<U, F>(&self, source: &mut Vec<U>, mut map: F) -> usize
    where
        F: FnMut(U) -> T,
    {
        let len = source.len();
        if len == 0 {
            return 0;
        }

        let mut tail = self.tail.0.load(Ordering::Relaxed);
        let head = self.head.0.load(Ordering::Acquire);
        let available = if tail >= head {
            self.slots() - 1 - (tail - head)
        } else {
            head - tail - 1
        };
        let accepted = len.min(available);
        if accepted == 0 {
            return 0;
        }

        let source_ptr = source.as_mut_ptr();
        for index in 0..accepted {
            let item = unsafe { source_ptr.add(index).read() };
            unsafe {
                (*self.buffer[tail].get()).write(map(item));
            }
            tail = self.next(tail);
        }

        let remaining = len - accepted;
        if remaining != 0 {
            unsafe {
                ptr::copy(source_ptr.add(accepted), source_ptr, remaining);
            }
        }
        unsafe {
            source.set_len(remaining);
        }
        self.tail.0.store(tail, Ordering::Release);
        accepted
    }

    #[inline]
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
    fn is_empty(&self) -> bool {
        self.head.0.load(Ordering::Acquire) == self.tail.0.load(Ordering::Acquire)
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
        let queue = Queue::<u32>::new(1);
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

    #[test]
    fn spsc_queue_pushes_many_from_vec() {
        let (mut producer, mut consumer) = spsc_pair(2);
        let mut source = vec![1, 2, 3];
        let accepted = unsafe { producer.push_many_from(&mut source, |value| value * 10) };
        assert_eq!(accepted, 2);
        assert_eq!(source, [3]);
        assert_eq!(consumer.pop(), Some(10));
        assert_eq!(consumer.pop(), Some(20));
        assert_eq!(consumer.pop(), None);
    }
}
