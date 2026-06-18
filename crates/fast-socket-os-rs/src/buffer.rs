//! Slab-backed packet buffers for the OS UDP backend.
//!
//! # Buffer lifetime contract
//!
//! OS packet buffers are [`Send`] so callers can allocate TX buffers on the
//! socket owner thread, fill them on worker threads, and then return them for
//! transmit. To keep that hot path free of mutexes and per-buffer reference
//! count traffic, buffers store raw pointers into pool-owned reclaim state.
//!
//! Every socket and buffer pool must outlive every buffer it hands out,
//! including buffers moved to other threads. Dropping a socket/pool while any of
//! its buffers still exist violates this backend invariant and would leave those
//! raw pointers dangling. Cross-thread buffer drops are supported by pushing
//! returned storage into a bounded MPSC remote reclaim queue that the owner
//! thread drains before reusing buffers.
//!
//! **This contract is not enforced by the type system: `recv`/`allocate` hand
//! out owned, `'static`, [`Send`] buffers, so safe code *can* drop the owning
//! socket first and then touch (or even just drop) a surviving buffer, which
//! is undefined behavior.** Debug builds catch this with an owner-generation
//! token checked on each byte access and on reclaim. The token and checks
//! compile to nothing in release builds unless the `buffer-guard` feature is
//! enabled.

use std::cell::{Cell, UnsafeCell};
use std::fmt;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::{self, ThreadId};

use crossbeam_queue::ArrayQueue;
use fast_socket_rs::{
    BufferAccessError, BufferLayout, OwnedPacketBuffer, PacketBuffer, PacketBufferMut,
    ReserveError, Segment, SegmentMut,
};

const SLAB_SIZE: usize = 64;

use self::owner_epoch::{BufferEpoch, OwnerEpoch};

/// "Is the owning socket/pool still alive?" tracking for raw buffer pointers.
#[cfg(any(debug_assertions, feature = "buffer-guard"))]
mod owner_epoch {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);
    const DEAD: u64 = 0;

    #[derive(Debug)]
    pub(super) struct OwnerEpoch {
        shared: Arc<AtomicU64>,
        generation: u64,
    }

    impl OwnerEpoch {
        pub(super) fn new() -> Self {
            let generation = NEXT_GENERATION.fetch_add(1, Ordering::Relaxed);
            Self {
                shared: Arc::new(AtomicU64::new(generation)),
                generation,
            }
        }

        pub(super) fn token(&self) -> BufferEpoch {
            BufferEpoch {
                shared: Arc::clone(&self.shared),
                generation: self.generation,
            }
        }
    }

    impl Drop for OwnerEpoch {
        fn drop(&mut self) {
            self.shared.store(DEAD, Ordering::Release);
        }
    }

    #[derive(Clone, Debug)]
    pub(super) struct BufferEpoch {
        shared: Arc<AtomicU64>,
        generation: u64,
    }

    impl BufferEpoch {
        #[inline]
        pub(super) fn assert_owner_alive(&self) {
            assert_eq!(
                self.shared.load(Ordering::Acquire),
                self.generation,
                "OS packet buffer used after its owning socket/pool was dropped: the \
                 socket/pool must outlive every buffer it hands out. The buffer holds raw \
                 pointers into pool-owned reclaim state that are now dangling.",
            );
        }
    }
}

/// Zero-sized no-op guard used in release builds without `buffer-guard`.
#[cfg(not(any(debug_assertions, feature = "buffer-guard")))]
mod owner_epoch {
    #[derive(Debug)]
    pub(super) struct OwnerEpoch;

    impl OwnerEpoch {
        #[inline]
        pub(super) fn new() -> Self {
            Self
        }

        #[inline]
        pub(super) fn token(&self) -> BufferEpoch {
            BufferEpoch
        }
    }

    #[derive(Clone, Debug)]
    pub(super) struct BufferEpoch;

    impl BufferEpoch {
        #[inline]
        pub(super) fn assert_owner_alive(&self) {}
    }
}

#[cfg(not(any(debug_assertions, feature = "buffer-guard")))]
const _: () = {
    assert!(core::mem::size_of::<OwnerEpoch>() == 0);
    assert!(core::mem::size_of::<BufferEpoch>() == 0);
};

#[derive(Debug)]
struct OsPoolInner {
    owner: ThreadId,
    free: UnsafeCell<Vec<Vec<u8>>>,
    remote: MpscQueue<Vec<u8>>,
    /// Total number of backing buffers issued by this pool (free + in-flight).
    /// Used to cap growth so a misbehaving caller cannot exhaust system memory
    /// by holding allocations forever.
    allocated: Cell<usize>,
    max_buffers: usize,
    epoch: OwnerEpoch,
}

// SAFETY: the local `free` vector and `allocated` counter are accessed only by
// the owner thread. Buffers dropped on other threads push into the bounded MPSC
// `remote` queue, which the owner drains before allocation/reuse.
unsafe impl Send for OsPoolInner {}
unsafe impl Sync for OsPoolInner {}

impl OsPoolInner {
    fn new(max_buffers: usize) -> Self {
        Self {
            owner: thread::current().id(),
            free: UnsafeCell::new(Vec::new()),
            remote: MpscQueue::new(max_buffers),
            allocated: Cell::new(0),
            max_buffers,
            epoch: OwnerEpoch::new(),
        }
    }

    /// Returns a liveness token for a buffer backed by this pool. Debug builds
    /// use it to detect use-after-pool-drop; release builds compile it away.
    fn buffer_epoch(&self) -> BufferEpoch {
        self.epoch.token()
    }

    fn pop(&self) -> Option<Vec<u8>> {
        debug_assert!(self.current_thread_owns());
        // SAFETY: pool allocation is owner-thread only.
        let free = unsafe { &mut *self.free.get() };
        if free.is_empty() {
            drain_remote(&self.remote, free);
        }
        free.pop()
    }

    fn push(&self, storage: Vec<u8>) {
        if self.current_thread_owns() {
            // SAFETY: owner-thread drops may push directly into the local free list.
            unsafe { &mut *self.free.get() }.push(storage);
        } else {
            self.remote.push(storage);
        }
    }

    /// Allocates another slab of backing buffers, capped at `max_buffers`
    /// total. Returns the number of buffers added so callers can detect
    /// exhaustion.
    fn grow(&self, layout: BufferLayout) -> usize {
        debug_assert!(self.current_thread_owns());
        let allocated = self.allocated.get();
        let remaining = self.max_buffers.saturating_sub(allocated);
        if remaining == 0 {
            return 0;
        }
        let chunk = SLAB_SIZE.min(remaining);
        let allocation_len = layout.allocation_len();
        // SAFETY: pool growth is owner-thread only.
        let free = unsafe { &mut *self.free.get() };
        free.reserve(chunk);
        for _ in 0..chunk {
            free.push(vec![0; allocation_len]);
        }
        self.allocated.set(allocated + chunk);
        chunk
    }

    fn current_thread_owns(&self) -> bool {
        current_thread_id() == self.owner
    }
}

#[derive(Debug)]
struct MpscQueue<T: Send> {
    inner: ArrayQueue<T>,
    len: AtomicUsize,
}

impl<T: Send> MpscQueue<T> {
    fn new(capacity: usize) -> Self {
        Self {
            inner: ArrayQueue::new(capacity.max(1)),
            len: AtomicUsize::new(0),
        }
    }

    fn push(&self, value: T) {
        self.len.fetch_add(1, Ordering::Release);
        if self.inner.push(value).is_err() {
            self.len.fetch_sub(1, Ordering::Relaxed);
            panic!("OS reclaim remote queue full; capacity must cover all in-flight buffers");
        }
    }

    fn is_empty(&self) -> bool {
        self.len.load(Ordering::Acquire) == 0
    }

    fn drain_into(&self, out: &mut Vec<T>) {
        let mut drained = 0usize;
        while let Some(value) = self.inner.pop() {
            out.push(value);
            drained += 1;
        }
        if drained > 0 {
            self.len.fetch_sub(drained, Ordering::Relaxed);
        }
    }
}

fn drain_remote<T: Send>(remote: &MpscQueue<T>, local: &mut Vec<T>) {
    if remote.is_empty() {
        return;
    }
    remote.drain_into(local);
}

#[inline]
fn current_thread_id() -> ThreadId {
    thread_local! {
        static CURRENT_THREAD_ID: ThreadId = thread::current().id();
    }
    CURRENT_THREAD_ID.with(|id| *id)
}

/// Slab-backed buffer pool used by [`crate::OsUdpSocket`].
///
/// Packet buffers are [`Send`], so storage can be returned to the pool from
/// another worker thread. The owner-thread path uses an unsynchronized local
/// free list; cross-thread drops enter a bounded remote reclaim queue.
#[derive(Clone)]
pub struct OsBufferPool {
    ctx: Rc<OsBufCtx>,
    reclaim: Rc<OsPoolInner>,
}

impl OsBufferPool {
    /// Creates a slab-backed pool for `layout` with the default pool limit.
    #[must_use]
    pub fn new(layout: BufferLayout) -> Self {
        Self::with_max_buffers(layout, crate::DEFAULT_POOL_MAX_BUFFERS)
    }

    /// Creates a slab-backed pool for `layout` with at most `max_buffers`
    /// backing buffers.
    ///
    /// A zero-sized limit is allowed and makes allocation return `None`
    /// immediately. [`crate::OsUdpSocket`] rejects zero-sized RX/TX pools
    /// because a live socket could not make progress with them.
    #[must_use]
    pub fn with_max_buffers(layout: BufferLayout, max_buffers: usize) -> Self {
        let layout = layout.with_max_segments(1);
        let reclaim = Rc::new(OsPoolInner::new(max_buffers));
        let ctx = Rc::new(OsBufCtx {
            layout,
            reclaim: NonNull::from(reclaim.as_ref()),
        });
        Self { ctx, reclaim }
    }
}

impl fmt::Debug for OsBufferPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OsBufferPool")
            .field("layout", &self.ctx.layout)
            .field("max_buffers", &self.reclaim.max_buffers)
            .finish_non_exhaustive()
    }
}

impl OsBufferPool {
    /// Returns the layout used for newly allocated buffers.
    #[must_use]
    pub fn layout(&self) -> &BufferLayout {
        &self.ctx.layout
    }

    /// Allocates one OS packet buffer.
    ///
    /// The returned buffer borrows this pool's reclaim state through raw
    /// pointers: **this pool and its owning socket must outlive the buffer**,
    /// even if it is moved to another thread. Debug builds panic on violation;
    /// release builds do not check.
    pub fn allocate(&mut self) -> Option<OsPacketBufMut> {
        let mut storage = match self.reclaim.pop() {
            Some(storage) => storage,
            None => {
                if self.reclaim.grow(self.ctx.layout) == 0 {
                    // Pool is at its cap with no free buffer available. Surface
                    // this as back-pressure instead of OOMing the host.
                    return None;
                }
                self.reclaim.pop()?
            }
        };

        let allocation_len = self.ctx.layout.allocation_len();
        if storage.len() != allocation_len {
            storage.resize(allocation_len, 0);
        }

        let data_offset = self.ctx.layout.data_offset();
        Some(OsPacketBufMut::from_storage(
            storage,
            NonNull::from(self.ctx.as_ref()),
            self.reclaim.buffer_epoch(),
            data_offset,
            data_offset,
        ))
    }
}

/// Per-pool constant state shared by every buffer that pool hands out.
#[derive(Debug)]
struct OsBufCtx {
    layout: BufferLayout,
    // SAFETY invariant: this pointer references the pool-owned `OsPoolInner`
    // allocation. The pool's `Rc<OsPoolInner>` keeps it alive for every buffer
    // that follows the module-level lifetime contract.
    reclaim: NonNull<OsPoolInner>,
}

// SAFETY: `OsBufCtx` is immutable after construction and points at
// `OsPoolInner`, whose split reclaim path is thread-safe under its documented
// owner-thread invariant.
unsafe impl Send for OsBufCtx {}
unsafe impl Sync for OsBufCtx {}

/// Owned OS packet buffer state shared by mutable and frozen handles.
#[derive(Debug)]
struct OsPacketBufInner {
    storage: Vec<u8>,
    // SAFETY invariant: raw pointer into pool-owned `Rc<OsBufCtx>` memory; the
    // pool/socket must outlive this buffer (module-level lifetime contract).
    ctx: NonNull<OsBufCtx>,
    /// Debug-only liveness token for the owning socket/pool. Checked before
    /// every `ctx` dereference and on reclaim.
    epoch: BufferEpoch,
    armed: bool,
    start: usize,
    end: usize,
    _not_sync: PhantomData<Cell<()>>,
}

impl OsPacketBufInner {
    fn from_storage(
        storage: Vec<u8>,
        ctx: NonNull<OsBufCtx>,
        epoch: BufferEpoch,
        start: usize,
        end: usize,
    ) -> Self {
        let inner = Self {
            storage,
            ctx,
            epoch,
            armed: true,
            start,
            end,
            _not_sync: PhantomData,
        };
        debug_assert!(end <= inner.storage.len());
        inner
    }

    #[inline]
    fn ctx(&self) -> &OsBufCtx {
        self.epoch.assert_owner_alive();
        // SAFETY: the pool/socket keeps the `Rc<OsBufCtx>` alive for the
        // lifetime of this buffer (module-level lifetime contract).
        unsafe { self.ctx.as_ref() }
    }

    #[inline]
    fn layout(&self) -> &BufferLayout {
        &self.ctx().layout
    }

    #[inline]
    fn storage(&self) -> &[u8] {
        self.epoch.assert_owner_alive();
        &self.storage
    }

    #[inline]
    fn storage_mut(&mut self) -> &mut [u8] {
        self.epoch.assert_owner_alive();
        &mut self.storage
    }

    fn reclaim(&mut self) {
        if !self.armed {
            return;
        }
        self.epoch.assert_owner_alive();
        // SAFETY: owner liveness asserted above; the pool keeps the ctx alive.
        let ctx = unsafe { self.ctx.as_ref() };
        let storage = std::mem::take(&mut self.storage);
        // SAFETY: the socket/pool keeps the `OsPoolInner` allocation alive for
        // the lifetime of this buffer. Remote-thread drops use its MPSC queue.
        unsafe { ctx.reclaim.as_ref() }.push(storage);
        self.armed = false;
    }
}

impl Drop for OsPacketBufInner {
    fn drop(&mut self) {
        if self.armed {
            self.reclaim();
        }
    }
}

/// Mutable OS UDP packet buffer.
///
/// # Lifetime contract
///
/// A buffer borrows socket/pool-owned reclaim state through raw pointers but is
/// an owned, [`Send`] value with no lifetime tying it to its socket. **The
/// socket and pools that produced this buffer must outlive it**, including
/// after it is moved to another thread. Debug builds panic on violation; release
/// builds do not check it.
#[repr(transparent)]
pub struct OsPacketBufMut {
    inner: OsPacketBufInner,
}

impl fmt::Debug for OsPacketBufMut {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OsPacketBufMut")
            .field("len", &self.len())
            .field("headroom", &self.headroom())
            .field("tailroom", &self.tailroom())
            .field("layout", self.layout())
            .finish_non_exhaustive()
    }
}

impl OsPacketBufMut {
    fn from_storage(
        storage: Vec<u8>,
        ctx: NonNull<OsBufCtx>,
        epoch: BufferEpoch,
        start: usize,
        end: usize,
    ) -> Self {
        Self {
            inner: OsPacketBufInner::from_storage(storage, ctx, epoch, start, end),
        }
    }

    /// Returns the current packet bytes as a contiguous slice.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.inner.storage()[self.inner.start..self.inner.end]
    }

    /// Returns the current packet bytes as a mutable contiguous slice.
    #[must_use]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        let start = self.inner.start;
        let end = self.inner.end;
        &mut self.inner.storage_mut()[start..end]
    }

    pub(crate) fn data_ptr(&mut self) -> *mut u8 {
        let data_offset = self.inner.layout().data_offset();
        unsafe { self.inner.storage_mut().as_mut_ptr().add(data_offset) }
    }

    pub(crate) fn data_capacity(&self) -> usize {
        self.inner.layout().payload_capacity()
    }

    pub(crate) fn set_received_len(&mut self, len: usize) -> Result<(), BufferAccessError> {
        let capacity = self.inner.layout().payload_capacity();
        if len > capacity {
            return Err(BufferAccessError::InsufficientTailroom {
                available: capacity,
                requested: len,
            });
        }
        self.inner.start = self.inner.layout().data_offset();
        self.inner.end = self.inner.start + len;
        Ok(())
    }
}

/// Immutable OS UDP packet buffer.
///
/// Carries the same [lifetime contract](OsPacketBufMut#lifetime-contract) as
/// [`OsPacketBufMut`]: the owning socket/pool must outlive the buffer.
#[repr(transparent)]
pub struct OsPacketBuf {
    inner: OsPacketBufInner,
}

impl fmt::Debug for OsPacketBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OsPacketBuf")
            .field("len", &self.len())
            .field("headroom", &self.headroom())
            .field("tailroom", &self.tailroom())
            .field("layout", self.layout())
            .finish_non_exhaustive()
    }
}

impl OsPacketBuf {
    /// Returns the packet bytes as a contiguous slice.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.inner.storage()[self.inner.start..self.inner.end]
    }
}

// SAFETY: OS buffers carry raw pointers into socket/pool-owned reclaim state.
// They may be moved to worker threads for filling or dropped there; remote
// drops enter a bounded MPSC reclaim queue. The socket and pools that created a
// buffer must outlive it, as documented in the module-level lifetime contract
// and enforced in debug builds by the owner-generation token. The `Cell` marker
// keeps buffers `!Sync`, so packet memory is not shared by reference across
// threads.
unsafe impl Send for OsPacketBufMut {}

// SAFETY: same invariant as `OsPacketBufMut`; freezing transfers ownership of
// the same backing storage into the immutable handle.
unsafe impl Send for OsPacketBuf {}

impl OwnedPacketBuffer for OsPacketBuf {
    type Mutable = OsPacketBufMut;

    fn into_mut(self) -> Self::Mutable {
        let this = ManuallyDrop::new(self);
        OsPacketBufMut {
            inner: unsafe { std::ptr::read(&this.inner) },
        }
    }
}

/// The `PacketBuffer` read surface is identical for the immutable and mutable
/// OS buffers, so emit it once for each type instead of duplicating the six
/// methods.
macro_rules! impl_os_packet_buffer {
    ($ty:ty) => {
        impl PacketBuffer for $ty {
            type Segments<'a> = std::option::IntoIter<Segment<'a>>;

            fn len(&self) -> usize {
                self.inner.end - self.inner.start
            }

            fn headroom(&self) -> usize {
                self.inner
                    .start
                    .checked_sub(self.inner.layout().l2_headroom())
                    .expect("packet start is above l2 headroom")
            }

            fn tailroom(&self) -> usize {
                self.inner.storage().len() - self.inner.end
            }

            fn layout(&self) -> &BufferLayout {
                self.inner.layout()
            }

            fn segments(&self) -> Self::Segments<'_> {
                (!self.is_empty()).then_some(self.as_slice()).into_iter()
            }

            fn first_segment(&self) -> Option<Segment<'_>> {
                (!self.is_empty()).then_some(self.as_slice())
            }

            fn contiguous(&self) -> Option<&[u8]> {
                Some(self.as_slice())
            }

            fn read_at_exact(
                &self,
                offset: usize,
                dst: &mut [u8],
            ) -> Result<(), BufferAccessError> {
                read_contiguous(self.as_slice(), offset, dst)
            }
        }
    };
}

impl_os_packet_buffer!(OsPacketBuf);
impl_os_packet_buffer!(OsPacketBufMut);

impl PacketBufferMut for OsPacketBufMut {
    type Frozen = OsPacketBuf;
    type SegmentsMut<'a> = std::option::IntoIter<SegmentMut<'a>>;

    fn segments_mut(&mut self) -> Self::SegmentsMut<'_> {
        (!self.is_empty())
            .then_some(self.as_mut_slice())
            .into_iter()
    }

    fn first_segment_mut(&mut self) -> Option<SegmentMut<'_>> {
        (!self.is_empty()).then_some(self.as_mut_slice())
    }

    fn contiguous_mut(&mut self) -> Option<&mut [u8]> {
        Some(self.as_mut_slice())
    }

    fn prepend(&mut self, bytes: &[u8]) -> Result<(), ReserveError> {
        if bytes.len() > self.headroom() {
            return Err(ReserveError::InsufficientHeadroom {
                available: self.headroom(),
                requested: bytes.len(),
            });
        }
        let new_start = self.inner.start - bytes.len();
        let start = self.inner.start;
        self.inner.storage_mut()[new_start..start].copy_from_slice(bytes);
        self.inner.start = new_start;
        Ok(())
    }

    fn extend_from_slice(&mut self, bytes: &[u8]) -> Result<(), BufferAccessError> {
        if bytes.len() > self.tailroom() {
            return Err(BufferAccessError::InsufficientTailroom {
                available: self.tailroom(),
                requested: bytes.len(),
            });
        }

        let end = self.inner.end;
        let next_end = end + bytes.len();
        self.inner.storage_mut()[end..next_end].copy_from_slice(bytes);
        self.inner.end = next_end;
        Ok(())
    }

    fn trim_prefix(&mut self, len: usize) -> Result<(), BufferAccessError> {
        if len > self.len() {
            return Err(BufferAccessError::OutOfBounds {
                offset: 0,
                len,
                packet_len: self.len(),
            });
        }
        self.inner.start += len;
        Ok(())
    }

    fn trim_suffix(&mut self, len: usize) -> Result<(), BufferAccessError> {
        if len > self.len() {
            return Err(BufferAccessError::OutOfBounds {
                offset: self.len().saturating_sub(len),
                len,
                packet_len: self.len(),
            });
        }
        self.inner.end -= len;
        Ok(())
    }

    fn freeze(self) -> Self::Frozen {
        let this = ManuallyDrop::new(self);
        OsPacketBuf {
            inner: unsafe { std::ptr::read(&this.inner) },
        }
    }
}

fn read_contiguous(packet: &[u8], offset: usize, dst: &mut [u8]) -> Result<(), BufferAccessError> {
    let Some(end) = offset.checked_add(dst.len()) else {
        return Err(BufferAccessError::OutOfBounds {
            offset,
            len: dst.len(),
            packet_len: packet.len(),
        });
    };

    let Some(src) = packet.get(offset..end) else {
        return Err(BufferAccessError::OutOfBounds {
            offset,
            len: dst.len(),
            packet_len: packet.len(),
        });
    };

    dst.copy_from_slice(src);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send<T: Send>() {}

    #[test]
    fn os_packet_buffers_are_send() {
        assert_send::<OsPacketBufMut>();
        assert_send::<OsPacketBuf>();
    }

    #[test]
    fn mutable_buffer_owner_thread_drop_recycles_storage() {
        let mut pool = OsBufferPool::with_max_buffers(BufferLayout::new(64), 1);
        let packet = pool.allocate().unwrap();

        assert!(pool.allocate().is_none());
        drop(packet);
        assert!(pool.allocate().is_some());
    }

    #[test]
    fn mutable_buffer_cross_thread_drop_recycles_storage() {
        let mut pool = OsBufferPool::with_max_buffers(BufferLayout::new(64), 1);
        let mut packet = pool.allocate().unwrap();
        packet.extend_from_slice(b"abc").unwrap();

        assert!(pool.allocate().is_none());
        std::thread::spawn(move || drop(packet)).join().unwrap();
        assert!(pool.allocate().is_some());
    }

    #[test]
    fn frozen_buffer_cross_thread_drop_recycles_storage() {
        let mut pool = OsBufferPool::with_max_buffers(BufferLayout::new(64), 1);
        let mut packet = pool.allocate().unwrap();
        packet.extend_from_slice(b"abc").unwrap();
        let packet = packet.freeze();

        assert!(pool.allocate().is_none());
        std::thread::spawn(move || {
            assert_eq!(packet.as_slice(), b"abc");
            drop(packet);
        })
        .join()
        .unwrap();
        assert!(pool.allocate().is_some());
    }

    #[cfg(any(debug_assertions, feature = "buffer-guard"))]
    #[test]
    #[should_panic(expected = "OS packet buffer used after its owning socket/pool was dropped")]
    fn drop_after_pool_drop_panics_with_buffer_guard() {
        let packet = {
            let mut pool = OsBufferPool::with_max_buffers(BufferLayout::new(64), 1);
            pool.allocate().unwrap()
        };

        drop(packet);
    }
}
