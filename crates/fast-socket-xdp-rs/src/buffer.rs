//! Allocation-free packet buffers for the AF_XDP backend.
//!
//! Live sockets hand TX buffers out of the UMEM TX frame free list and wrap RX
//! descriptors directly as packet buffers. The fallback heap mode is kept for
//! unprivileged tests and uses a small recycled frame pool instead of allocating
//! on every `allocate()` call.
//!
//! # Buffer lifetime contract
//!
//! XDP packet buffers are `Send` so callers can allocate TX buffers on the
//! socket owner thread, fill them on worker threads, and then return them to the
//! owner for transmit. To keep that hot path allocation-free, live buffers store
//! raw pointers into socket-owned UMEM and reclaim state instead of cloning
//! reference-counted handles per packet.
//!
//! That means every socket and buffer pool must outlive every buffer it hands
//! out, including buffers moved to other threads. Dropping a socket/pool while
//! any of its buffers still exist violates this backend invariant and would
//! leave those raw pointers dangling. Cross-thread buffer drops are supported by
//! pushing returned frames into a bounded MPSC remote reclaim queue that the
//! owner thread drains before reusing frames.
//!
//! **This contract is not enforced by the type system: `recv`/`allocate` hand
//! out owned, `'static`, [`Send`] buffers, so safe code *can* drop the owning
//! socket first and then touch (or even just drop) a surviving buffer — which
//! is undefined behavior.** Debug builds catch exactly this: every buffer holds
//! an owner-generation token (see the owner-epoch guard below) that is checked
//! on each byte access and on reclaim, so the misuse panics with a clear message
//! instead of silently reading or writing freed memory. The token and all its
//! checks compile to nothing in release builds.

use std::cell::{Cell, UnsafeCell};
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::ptr;
use std::ptr::NonNull;
use std::rc::Rc;
use std::slice;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::{self, ThreadId};

use crossbeam_queue::ArrayQueue;
use fast_socket_rs::{
    BufferAccessError, BufferLayout, Error, OwnedPacketBuffer, PacketBuffer, PacketBufferMut,
    ReserveError, Segment,
};

use crate::ring::XdpDesc;
use crate::umem::Umem;

use self::owner_epoch::{BufferEpoch, OwnerEpoch};

/// "Is the owning socket/pool still alive?" tracking for the raw pointers each
/// live buffer holds.
///
/// XDP buffers are [`Send`] and store raw `NonNull` pointers into socket/pool-
/// owned UMEM and reclaim state (see the module-level lifetime contract). If the
/// owning socket/pool is dropped while a buffer is still alive, those pointers
/// dangle and any later use — reading the bytes, *or even the buffer's own
/// `Drop`* — is undefined behavior.
///
/// When the guard is active it makes that misuse loud instead of silent: an
/// owner takes a unique generation, shares it (behind an `Arc`, so it survives
/// the owner), and stamps it dead on drop; every buffer captures the generation
/// at creation and asserts it still matches before each access and on reclaim.
///
/// The guard is active in debug builds **or** when the `buffer-guard` crate
/// feature is enabled (e.g. for a hardened release / canary build). Otherwise
/// every type here is a zero-sized no-op: the checks compile away and embedding
/// the epoch adds no bytes to any struct (enforced by the `const` size assertion
/// below).
#[cfg(any(debug_assertions, feature = "buffer-guard"))]
mod owner_epoch {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);
    /// Stored into the shared cell when the owner drops. Real generations start
    /// at 1, so a buffer that reads 0 knows its owner is gone.
    const DEAD: u64 = 0;

    /// Liveness epoch held by an owner (a reclaim pool); marks the shared cell
    /// dead on drop.
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

        /// Returns a token for a buffer this owner hands out.
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

    /// Liveness token captured by a buffer at creation; checked on access and
    /// on reclaim.
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
                "XDP packet buffer used after its owning socket/pool was dropped: the \
                 socket/pool must outlive every buffer it hands out (see the `buffer` module \
                 lifetime contract). The buffer holds raw pointers into socket-owned UMEM and \
                 reclaim state that are now dangling.",
            );
        }
    }
}

/// Zero-sized no-op variant used when the buffer guard is disabled. Every method
/// is a no-op and both types are zero-sized, so embedding them costs nothing.
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

/// With the guard disabled, the epoch types must be zero-sized so embedding them
/// adds no bytes to `FrameReclaim`/`HeapReclaim`/`XdpStorage`. Enforced at
/// compile time: a future field that breaks this fails the build rather than
/// silently growing the hot-path buffer structs.
#[cfg(not(any(debug_assertions, feature = "buffer-guard")))]
const _: () = {
    assert!(core::mem::size_of::<OwnerEpoch>() == 0);
    assert!(core::mem::size_of::<BufferEpoch>() == 0);
};

/// Iterator over XDP packet segments.
#[derive(Clone, Debug)]
pub struct XdpSegments<'a> {
    segment: Option<&'a [u8]>,
}

impl<'a> XdpSegments<'a> {
    fn one(segment: &'a [u8]) -> Self {
        Self {
            segment: Some(segment),
        }
    }

    fn empty() -> Self {
        Self { segment: None }
    }
}

impl<'a> Iterator for XdpSegments<'a> {
    type Item = Segment<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.segment.take()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = usize::from(self.segment.is_some());
        (len, Some(len))
    }
}

impl ExactSizeIterator for XdpSegments<'_> {}

#[derive(Debug)]
struct HeapReclaim {
    owner: ThreadId,
    free: UnsafeCell<Vec<Box<[u8]>>>,
    remote: MpscQueue<Box<[u8]>>,
    epoch: OwnerEpoch,
}

#[derive(Debug)]
pub(crate) struct FrameReclaim {
    owner: ThreadId,
    free: UnsafeCell<Vec<u64>>,
    remote: MpscQueue<u64>,
    epoch: OwnerEpoch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct XdpPreparedTxBatch {
    pub(crate) prepared: usize,
    pub(crate) tx_bytes: u64,
}

// SAFETY: the `free` vectors are accessed only by the owner thread. Buffers
// dropped on other threads push into `remote`, which is a bounded MPSC queue;
// the owner drains `remote` back into `free` before allocation/reuse.
unsafe impl Send for HeapReclaim {}
unsafe impl Sync for HeapReclaim {}

// SAFETY: same split-reclaim invariant as `HeapReclaim`.
unsafe impl Send for FrameReclaim {}
unsafe impl Sync for FrameReclaim {}

impl HeapReclaim {
    fn new(layout: BufferLayout, count: usize) -> Rc<Self> {
        let mut free = Vec::with_capacity(count);
        for _ in 0..count {
            free.push(vec![0; layout.chunk_size()].into_boxed_slice());
        }
        Rc::new(Self {
            owner: thread::current().id(),
            free: UnsafeCell::new(free),
            remote: MpscQueue::new(count),
            epoch: OwnerEpoch::new(),
        })
    }

    /// Returns a liveness token for a buffer backed by this pool. Debug builds
    /// use it to detect use-after-pool-drop; release builds compile it away.
    fn buffer_epoch(&self) -> BufferEpoch {
        self.epoch.token()
    }

    fn pop(&self) -> Option<Box<[u8]>> {
        // SAFETY: pool allocation is owner-thread only.
        let free = unsafe { &mut *self.free.get() };
        if free.is_empty() {
            drain_remote(&self.remote, free);
        }
        free.pop()
    }

    fn push(&self, frame: Box<[u8]>) {
        if self.current_thread_owns() {
            // SAFETY: owner-thread drops may push directly into the local free list.
            unsafe { &mut *self.free.get() }.push(frame);
        } else {
            self.remote.push(frame);
        }
    }

    fn current_thread_owns(&self) -> bool {
        current_thread_id() == self.owner
    }
}

impl FrameReclaim {
    pub(crate) fn new(frames: Vec<u64>) -> Rc<Self> {
        Self::with_remote_capacity(frames, None)
    }

    pub(crate) fn new_with_remote_capacity(frames: Vec<u64>, remote_capacity: usize) -> Rc<Self> {
        Self::with_remote_capacity(frames, Some(remote_capacity))
    }

    fn with_remote_capacity(frames: Vec<u64>, remote_capacity: Option<usize>) -> Rc<Self> {
        let remote_capacity = remote_capacity.unwrap_or_else(|| frames.capacity());
        Rc::new(Self {
            owner: thread::current().id(),
            free: UnsafeCell::new(frames),
            remote: MpscQueue::new(remote_capacity),
            epoch: OwnerEpoch::new(),
        })
    }

    /// Returns a liveness token for a buffer backed by this pool. Debug builds
    /// use it to detect use-after-pool-drop; release builds compile it away.
    fn buffer_epoch(&self) -> BufferEpoch {
        self.epoch.token()
    }

    fn pop(&self) -> Option<u64> {
        // SAFETY: pool allocation is owner-thread only.
        let free = unsafe { &mut *self.free.get() };
        if free.is_empty() {
            drain_remote(&self.remote, free);
        }
        free.pop()
    }

    pub(crate) fn pop_many_with(&self, max: usize, mut f: impl FnMut(u64)) -> usize {
        if max == 0 {
            return 0;
        }

        // SAFETY: pool allocation is owner-thread only.
        let free = unsafe { &mut *self.free.get() };
        if free.len() < max {
            drain_remote(&self.remote, free);
        }
        let count = max.min(free.len());
        for _ in 0..count {
            f(free.pop().expect("count is bounded by free.len()"));
        }
        count
    }

    pub(crate) fn push(&self, addr: u64) {
        if self.current_thread_owns() {
            // SAFETY: owner-thread drops/completions may push directly into the
            // local free list.
            unsafe { &mut *self.free.get() }.push(addr);
        } else {
            self.remote.push(addr);
        }
    }

    pub(crate) fn push_local(&self, addr: u64) {
        // SAFETY: used by owner-thread completion reclaim paths.
        unsafe { &mut *self.free.get() }.push(addr);
    }

    pub(crate) fn is_empty(&self) -> bool {
        // SAFETY: owner-thread only helper.
        let free = unsafe { &mut *self.free.get() };
        if free.is_empty() {
            drain_remote(&self.remote, free);
        }
        free.is_empty()
    }

    pub(crate) fn drain_into(&self, out: &mut Vec<u64>) {
        // SAFETY: owner-thread only helper.
        let free = unsafe { &mut *self.free.get() };
        drain_remote(&self.remote, free);
        out.append(free);
    }

    fn current_thread_owns(&self) -> bool {
        current_thread_id() == self.owner
    }
}

#[derive(Debug)]
struct MpscQueue<T: Send> {
    inner: ArrayQueue<T>,
    // Pending or imminently pending entries in `inner`. Cross-thread pushes
    // reserve a slot before publishing into `inner`; the owner thread
    // decrements as it drains. The drain gate loads this with `Acquire` to
    // pair with the producer's `Release` increment, so once the owner observes
    // a non-zero count the corresponding `inner.push` is visible to its `pop`.
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
            panic!("XDP reclaim remote queue full; capacity must cover all in-flight buffers");
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

/// Receive pool for AF_XDP IP packet sockets.
#[derive(Debug)]
pub struct XdpRxPool {
    ctx: Rc<XdpBufCtx>,
    heap: Rc<HeapReclaim>,
    live: Option<XdpLivePool>,
}

/// Transmit pool for AF_XDP IP packet sockets.
#[derive(Debug)]
pub struct XdpTxPool {
    ctx: Rc<XdpBufCtx>,
    heap: Rc<HeapReclaim>,
    live: Option<XdpLivePool>,
}

#[derive(Clone)]
struct XdpLivePool {
    umem: Rc<Umem>,
    reclaim: Rc<FrameReclaim>,
}

impl fmt::Debug for XdpLivePool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("XdpLivePool").finish_non_exhaustive()
    }
}

impl XdpRxPool {
    /// Creates a receive pool with a small fallback heap cache.
    #[must_use]
    pub fn new(layout: BufferLayout) -> Self {
        Self::with_heap_capacity(layout, 64)
    }

    /// Creates a receive pool with `count` recycled heap frames.
    #[must_use]
    pub fn with_heap_capacity(layout: BufferLayout, count: usize) -> Self {
        let heap = HeapReclaim::new(layout, count);
        // The `Rc<HeapReclaim>` keeps this allocation alive for the lifetime of
        // the pool, so the captured `NonNull` is stable. Buffers hold a raw
        // pointer to the `XdpBufCtx`, which is itself kept alive by the pool's
        // `Rc<XdpBufCtx>`; see the module-level lifetime contract.
        let ctx = Rc::new(XdpBufCtx {
            layout,
            reclaim: XdpReclaimCtx::Heap(NonNull::from(heap.as_ref())),
        });
        Self {
            ctx,
            heap,
            live: None,
        }
    }

    pub(crate) fn live(layout: BufferLayout, umem: Rc<Umem>, reclaim: Rc<FrameReclaim>) -> Self {
        let heap = HeapReclaim::new(layout, 0);
        // The pool-owned `Rc`s keep the UMEM/reclaim allocations alive for the
        // lifetime of the pool, so the captured `NonNull`s are stable.
        let ctx = Rc::new(XdpBufCtx {
            layout,
            reclaim: XdpReclaimCtx::Umem {
                umem: NonNull::from(umem.as_ref()),
                reclaim: NonNull::from(reclaim.as_ref()),
                frame_size: umem.frame_size() as usize,
            },
        });
        Self {
            ctx,
            heap,
            live: Some(XdpLivePool { umem, reclaim }),
        }
    }

    pub(crate) fn wrap_rx_frame(
        &self,
        desc_addr: u64,
        packet_offset: usize,
        len: usize,
    ) -> Option<XdpPacketBufMut> {
        let live = self.live.as_ref()?;
        let frame_size = live.umem.frame_size() as u64;
        let frame_addr = desc_addr - (desc_addr % frame_size);
        if !live.umem.contains_frame_addr(frame_addr) {
            return None;
        }
        let desc_offset = (desc_addr - frame_addr) as usize;
        let start = desc_offset.checked_add(packet_offset)?;
        if start < self.ctx.layout.l2_headroom() || start.checked_add(len)? > frame_size as usize {
            return None;
        }
        // The pool-owned `Rc`s keep these allocations alive. Returned buffers
        // may cross threads, but the socket/pool must outlive them all; see the
        // module-level lifetime contract.
        Some(XdpPacketBufMut::from_storage(
            XdpStorage::Umem { frame_addr },
            NonNull::from(self.ctx.as_ref()),
            live.reclaim.buffer_epoch(),
            start,
            start + len,
        ))
    }
}

impl XdpTxPool {
    /// Creates a transmit pool with a small fallback heap cache.
    #[must_use]
    pub fn new(layout: BufferLayout) -> Self {
        Self::with_heap_capacity(layout, 64)
    }

    /// Creates a transmit pool with `count` recycled heap frames.
    #[must_use]
    pub fn with_heap_capacity(layout: BufferLayout, count: usize) -> Self {
        let heap = HeapReclaim::new(layout, count);
        // The `Rc<HeapReclaim>` keeps this allocation alive for the lifetime of
        // the pool, so the captured `NonNull` is stable. See the module-level
        // lifetime contract.
        let ctx = Rc::new(XdpBufCtx {
            layout,
            reclaim: XdpReclaimCtx::Heap(NonNull::from(heap.as_ref())),
        });
        Self {
            ctx,
            heap,
            live: None,
        }
    }

    pub(crate) fn live(layout: BufferLayout, umem: Rc<Umem>, reclaim: Rc<FrameReclaim>) -> Self {
        let heap = HeapReclaim::new(layout, 0);
        // The pool-owned `Rc`s keep the UMEM/reclaim allocations alive for the
        // lifetime of the pool, so the captured `NonNull`s are stable.
        let ctx = Rc::new(XdpBufCtx {
            layout,
            reclaim: XdpReclaimCtx::Umem {
                umem: NonNull::from(umem.as_ref()),
                reclaim: NonNull::from(reclaim.as_ref()),
                frame_size: umem.frame_size() as usize,
            },
        });
        Self {
            ctx,
            heap,
            live: Some(XdpLivePool { umem, reclaim }),
        }
    }

    /// Test-only single-frame reclaim. The hot completion-drain path goes
    /// through [`Self::live_reclaim`] and pushes directly into the
    /// underlying [`FrameReclaim`], skipping the per-frame `Option` check.
    #[cfg(test)]
    pub(crate) fn reclaim_completed_frame(&mut self, frame_addr: u64) {
        if let Some(live) = &self.live {
            debug_assert!(live.umem.contains_frame_addr(frame_addr));
            live.reclaim.push_local(frame_addr);
        }
    }

    /// Lends the shared reclaim queue for the live UMEM-backed half of this
    /// pool, if any. Bulk completion-drain code grabs this once and pushes
    /// per frame.
    pub(crate) fn live_reclaim(&self) -> Option<&FrameReclaim> {
        self.live.as_ref().map(|live| live.reclaim.as_ref())
    }

    pub(crate) fn allocate_many(&mut self, out: &mut Vec<XdpPacketBufMut>, max: usize) -> usize {
        if max == 0 {
            return 0;
        }

        if let Some(live) = &self.live {
            // The pool-owned `Rc`s keep these allocations alive. Returned
            // buffers may cross threads, but the socket/pool must outlive them
            // all; see the module-level lifetime contract.
            let ctx_ptr = NonNull::from(self.ctx.as_ref());
            let data_offset = self.ctx.layout.data_offset();
            // Taken once; cloned per frame inside the closure (a no-op clone in
            // release builds, where `BufferEpoch` is zero-sized).
            let epoch = live.reclaim.buffer_epoch();
            out.reserve(max);

            let start_len = out.len();
            let out_ptr = out.as_mut_ptr();
            let mut written = 0usize;
            let allocated = live.reclaim.pop_many_with(max, |frame_addr| {
                // SAFETY: `reserve(max)` guarantees at least `max` spare
                // slots after `start_len`, and `pop_many_with` invokes this
                // closure at most `max` times. We set the vector length once
                // after all initialized tail elements have been written.
                unsafe {
                    out_ptr
                        .add(start_len + written)
                        .write(XdpPacketBufMut::from_storage(
                            XdpStorage::Umem { frame_addr },
                            ctx_ptr,
                            epoch.clone(),
                            data_offset,
                            data_offset,
                        ));
                }
                written += 1;
            });
            debug_assert_eq!(allocated, written);
            // SAFETY: exactly `written` tail elements were initialized above.
            unsafe {
                out.set_len(start_len + written);
            }
            return written;
        }

        let start_len = out.len();
        while out.len() - start_len < max {
            let Some(buffer) = self.allocate() else {
                break;
            };
            out.push(buffer);
        }
        out.len() - start_len
    }

    pub(crate) fn prepare_endpoint_batch<F>(
        &mut self,
        out: &mut Vec<XdpDesc>,
        header: &[u8],
        l2_len: usize,
        payload_capacity: usize,
        max: usize,
        mut prepare_frame: F,
    ) -> Result<XdpPreparedTxBatch, Error>
    where
        F: FnMut(usize, &mut [u8], &mut [u8]) -> Result<usize, Error>,
    {
        if max == 0 {
            return Ok(XdpPreparedTxBatch {
                prepared: 0,
                tx_bytes: 0,
            });
        }

        let live = self.live.as_ref().ok_or(Error::InvalidPacket)?;
        let payload_start = self.ctx.layout.data_offset();
        let header_start = payload_start
            .checked_sub(header.len())
            .ok_or(Error::InvalidPacket)?;
        let max_frame_len = header
            .len()
            .checked_add(payload_capacity)
            .ok_or(Error::InvalidPacket)?;
        let frame_size = live.umem.frame_size() as usize;
        if header_start
            .checked_add(max_frame_len)
            .ok_or(Error::InvalidPacket)?
            > frame_size
            || header.len() < l2_len
        {
            return Err(Error::InvalidPacket);
        }
        let _max_desc_len = u32::try_from(max_frame_len).map_err(|_| Error::InvalidPacket)?;
        let l3_header_len = header
            .len()
            .checked_sub(l2_len)
            .ok_or(Error::InvalidPacket)?;
        let umem_base = live.umem.as_ptr() as *mut u8;
        out.reserve(max);

        let start_len = out.len();
        let mut written = 0usize;
        let mut tx_bytes = 0u64;
        while written < max {
            let Some(frame_addr) = live.reclaim.pop() else {
                break;
            };

            // SAFETY: frame addresses come from this pool's live reclaim list,
            // and bounds above prove the header and payload-capacity slices fit.
            let payload_len = unsafe {
                let frame = umem_base.add(frame_addr as usize);
                ptr::copy_nonoverlapping(header.as_ptr(), frame.add(header_start), header.len());
                let header = slice::from_raw_parts_mut(frame.add(header_start), header.len());
                let payload = slice::from_raw_parts_mut(frame.add(payload_start), payload_capacity);
                prepare_frame(written, header, payload)
            };
            let payload_len = match payload_len {
                Ok(payload_len) if payload_len <= payload_capacity => payload_len,
                Ok(_) => {
                    live.reclaim.push_local(frame_addr);
                    for desc in out.drain(start_len..) {
                        live.reclaim.push_local(desc.addr - header_start as u64);
                    }
                    return Err(Error::OversizeForMtu);
                }
                Err(error) => {
                    live.reclaim.push_local(frame_addr);
                    for desc in out.drain(start_len..) {
                        live.reclaim.push_local(desc.addr - header_start as u64);
                    }
                    return Err(error);
                }
            };
            let desc_len = u32::try_from(header.len() + payload_len)
                .expect("payload length was bounded by max_desc_len");
            tx_bytes = tx_bytes.saturating_add((l3_header_len + payload_len) as u64);
            out.push(XdpDesc {
                addr: frame_addr + header_start as u64,
                len: desc_len,
                options: 0,
            });
            written += 1;
        }

        Ok(XdpPreparedTxBatch {
            prepared: written,
            tx_bytes,
        })
    }

    #[cfg(test)]
    pub(crate) fn reclaim_completed(&mut self, desc_addr: u64) {
        if let Some(live) = &self.live {
            let frame_size = live.umem.frame_size() as u64;
            let frame_addr = desc_addr - (desc_addr % frame_size);
            live.reclaim.push_local(frame_addr);
        }
    }
}

impl XdpRxPool {
    /// Returns the layout used for newly allocated buffers.
    #[must_use]
    pub fn layout(&self) -> &BufferLayout {
        &self.ctx.layout
    }

    /// Allocates one receive buffer.
    ///
    /// The returned buffer borrows this pool's frame storage through raw
    /// pointers (see [`XdpPacketBufMut`]'s lifetime contract): **this pool and
    /// its owning socket must outlive the buffer**, even if it is moved to
    /// another thread. Debug builds panic on violation; release builds do not
    /// check.
    pub fn allocate(&mut self) -> Option<XdpPacketBufMut> {
        let frame = self.heap.pop()?;
        let data_offset = self.ctx.layout.data_offset();
        Some(XdpPacketBufMut::from_storage(
            XdpStorage::Heap { frame },
            NonNull::from(self.ctx.as_ref()),
            self.heap.buffer_epoch(),
            data_offset,
            data_offset,
        ))
    }
}

impl XdpTxPool {
    /// Returns the layout used for newly allocated buffers.
    #[must_use]
    pub fn layout(&self) -> &BufferLayout {
        &self.ctx.layout
    }

    /// Allocates one transmit buffer.
    ///
    /// The returned buffer borrows this pool's UMEM/frame storage through raw
    /// pointers (see [`XdpPacketBufMut`]'s lifetime contract): **this pool and
    /// its owning socket must outlive the buffer**, even if it is moved to
    /// another thread. Debug builds panic on violation; release builds do not
    /// check.
    pub fn allocate(&mut self) -> Option<XdpPacketBufMut> {
        if let Some(live) = &self.live {
            let frame_addr = live.reclaim.pop()?;
            // See allocate_many: the socket/pool must outlive all buffers.
            let data_offset = self.ctx.layout.data_offset();
            return Some(XdpPacketBufMut::from_storage(
                XdpStorage::Umem { frame_addr },
                NonNull::from(self.ctx.as_ref()),
                live.reclaim.buffer_epoch(),
                data_offset,
                data_offset,
            ));
        }

        let frame = self.heap.pop()?;
        let data_offset = self.ctx.layout.data_offset();
        Some(XdpPacketBufMut::from_storage(
            XdpStorage::Heap { frame },
            NonNull::from(self.ctx.as_ref()),
            self.heap.buffer_epoch(),
            data_offset,
            data_offset,
        ))
    }
}

/// Per-pool constant state shared by every buffer that pool hands out.
///
/// One of these lives behind the pool's `Rc<XdpBufCtx>`; each buffer holds a raw
/// `NonNull<XdpBufCtx>` into it instead of duplicating the layout and reclaim
/// pointers per buffer. The pool/socket that owns the `Rc` must outlive every
/// buffer (see the module-level lifetime contract); the buffer's debug epoch
/// token guards against use-after-owner-drop exactly as the raw UMEM/reclaim
/// pointers did before.
#[derive(Debug)]
struct XdpBufCtx {
    layout: BufferLayout,
    reclaim: XdpReclaimCtx,
}

enum XdpReclaimCtx {
    // SAFETY invariant: this pointer references the pool-owned `HeapReclaim`
    // allocation. The pool's `Rc<HeapReclaim>` keeps it alive for the lifetime
    // of every buffer (see the module-level lifetime contract).
    Heap(NonNull<HeapReclaim>),
    // SAFETY invariant: `umem` and `reclaim` point at allocations owned by the
    // socket/pool that handed the buffer out. Buffers are `Send` and may be
    // filled or dropped on worker threads, so the owner socket/pool must
    // outlive every outstanding buffer. Cross-thread drops push into the
    // reclaim object's remote MPSC queue; owner-thread drops use its local free
    // list.
    //
    // Review item **S4** (UMEM lifetime is enforced only by docs) is
    // intentionally left as-is: switching the owning side to `Arc<Umem>` and
    // `Arc<FrameReclaim>` would either force every recv/send into an atomic
    // ref-count cycle on the steady-state hot path or leak the UMEM until the
    // last in-flight buffer drains. The current contract — "do not drop the
    // owning socket while there are outstanding buffers" — matches how every
    // backend in this workspace already uses the type.
    Umem {
        umem: NonNull<Umem>,
        reclaim: NonNull<FrameReclaim>,
        frame_size: usize,
    },
}

impl fmt::Debug for XdpReclaimCtx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Heap(_) => f.debug_struct("Heap").finish_non_exhaustive(),
            Self::Umem { frame_size, .. } => f
                .debug_struct("Umem")
                .field("frame_size", frame_size)
                .finish_non_exhaustive(),
        }
    }
}

/// Per-buffer frame data. All per-pool constant state lives in [`XdpBufCtx`];
/// this carries only what differs between buffers.
enum XdpStorage {
    Heap { frame: Box<[u8]> },
    Umem { frame_addr: u64 },
}

impl fmt::Debug for XdpStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Heap { frame } => f
                .debug_struct("Heap")
                .field("len", &frame.len())
                .finish_non_exhaustive(),
            Self::Umem { frame_addr } => f
                .debug_struct("Umem")
                .field("frame_addr", frame_addr)
                .finish_non_exhaustive(),
        }
    }
}

/// Owned AF_XDP packet buffer state shared by mutable and frozen handles.
#[derive(Debug)]
struct XdpPacketBufInner {
    storage: Option<XdpStorage>,
    // SAFETY invariant: raw pointer into pool-owned `Rc<XdpBufCtx>` memory; the
    // pool/socket must outlive this buffer (module-level lifetime contract).
    // The `epoch` token guards against use-after-owner-drop in debug builds.
    ctx: NonNull<XdpBufCtx>,
    /// Debug-only liveness token for the owning socket/pool (see
    /// [`owner_epoch`]). Checked before every `ctx`/UMEM deref and on reclaim so
    /// "socket dropped while this buffer was alive" panics instead of silently
    /// reading/writing freed memory.
    epoch: BufferEpoch,
    armed: bool,
    start: usize,
    end: usize,
    _not_sync: PhantomData<Cell<()>>,
}

impl XdpPacketBufInner {
    /// Returns the shared per-pool context.
    ///
    /// Asserts owner liveness because `ctx` is a raw pointer into pool-owned
    /// memory (same contract as the old umem/reclaim pointers); debug builds
    /// trip a clear assert here if the owning socket/pool was already dropped,
    /// release builds compile this to nothing.
    #[inline]
    fn ctx(&self) -> &XdpBufCtx {
        self.epoch.assert_owner_alive();
        // SAFETY: the pool/socket keeps the `Rc<XdpBufCtx>` alive for the
        // lifetime of this buffer (module-level lifetime contract).
        unsafe { self.ctx.as_ref() }
    }

    fn ptr(&self) -> *const u8 {
        match self.storage.as_ref().expect("buffer storage is present") {
            XdpStorage::Heap { frame } => {
                // Heap frames are owned inline; still assert owner liveness for
                // a uniform contract across both storage kinds.
                self.epoch.assert_owner_alive();
                frame.as_ptr()
            }
            XdpStorage::Umem { frame_addr } => {
                let frame_addr = *frame_addr;
                match &self.ctx().reclaim {
                    XdpReclaimCtx::Umem { umem, .. } => {
                        // SAFETY: the socket/pool keeps the UMEM allocation
                        // alive for the lifetime of this buffer (see
                        // XdpReclaimCtx::Umem invariant), and `frame_addr` is
                        // produced from its frame table.
                        unsafe { umem.as_ref().as_ptr().add(frame_addr as usize) }
                    }
                    XdpReclaimCtx::Heap(_) => {
                        unreachable!("Umem storage always pairs with Umem reclaim context")
                    }
                }
            }
        }
    }

    fn mut_ptr(&mut self) -> *mut u8 {
        // For Umem storage we need `ctx()` (which asserts); for Heap we assert
        // explicitly. Resolve the UMEM base pointer first to avoid borrowing
        // `self` mutably while `ctx()` borrows it immutably.
        match self.storage.as_ref().expect("buffer storage is present") {
            XdpStorage::Umem { frame_addr } => {
                let frame_addr = *frame_addr;
                match &self.ctx().reclaim {
                    XdpReclaimCtx::Umem { umem, .. } => {
                        // SAFETY: same UMEM-liveness invariant as `ptr`. Even
                        // when the buffer moves to another thread, Rust
                        // ownership gives that thread exclusive access to this
                        // packet frame.
                        unsafe { (umem.as_ref().as_ptr() as *mut u8).add(frame_addr as usize) }
                    }
                    XdpReclaimCtx::Heap(_) => {
                        unreachable!("Umem storage always pairs with Umem reclaim context")
                    }
                }
            }
            XdpStorage::Heap { .. } => {
                self.epoch.assert_owner_alive();
                match self.storage.as_mut().expect("buffer storage is present") {
                    XdpStorage::Heap { frame } => frame.as_mut_ptr(),
                    XdpStorage::Umem { .. } => unreachable!(),
                }
            }
        }
    }

    fn frame_capacity(&self) -> usize {
        match self.storage.as_ref().expect("buffer storage is present") {
            XdpStorage::Heap { frame } => frame.len(),
            XdpStorage::Umem { .. } => match &self.ctx().reclaim {
                XdpReclaimCtx::Umem { frame_size, .. } => *frame_size,
                XdpReclaimCtx::Heap(_) => {
                    unreachable!("Umem storage always pairs with Umem reclaim context")
                }
            },
        }
    }

    fn frame_addr(&self) -> Option<u64> {
        match self.storage.as_ref().expect("buffer storage is present") {
            XdpStorage::Umem { frame_addr } => Some(*frame_addr),
            XdpStorage::Heap { .. } => None,
        }
    }

    fn is_umem(&self) -> bool {
        matches!(
            self.storage.as_ref().expect("buffer storage is present"),
            XdpStorage::Umem { .. }
        )
    }

    fn reclaim(&mut self) {
        if !self.armed {
            return;
        }
        // The reclaim push dereferences the owner's reclaim pool; in debug
        // builds this fires a clear assert if that owner was already dropped
        // (otherwise a silent dangling-pointer write). Release: compiled away.
        self.epoch.assert_owner_alive();
        // SAFETY: owner liveness asserted above; pool keeps the ctx alive.
        let ctx = unsafe { self.ctx.as_ref() };
        match self.storage.as_mut().expect("buffer storage is present") {
            XdpStorage::Heap { frame } => match &ctx.reclaim {
                XdpReclaimCtx::Heap(reclaim) => {
                    let empty = Vec::new().into_boxed_slice();
                    let frame = mem::replace(frame, empty);
                    // SAFETY: same socket/pool-outlives-buffer invariant as
                    // live UMEM storage.
                    unsafe { reclaim.as_ref() }.push(frame);
                }
                XdpReclaimCtx::Umem { .. } => {
                    unreachable!("Heap storage always pairs with Heap reclaim context")
                }
            },
            XdpStorage::Umem { frame_addr } => match &ctx.reclaim {
                XdpReclaimCtx::Umem { reclaim, .. } => {
                    // SAFETY: the socket/pool keeps the FrameReclaim allocation
                    // alive for the lifetime of this buffer (see
                    // XdpReclaimCtx::Umem invariant). Remote-thread drops use
                    // the reclaim object's bounded MPSC queue.
                    unsafe { reclaim.as_ref() }.push(*frame_addr);
                }
                XdpReclaimCtx::Heap(_) => {
                    unreachable!("Umem storage always pairs with Umem reclaim context")
                }
            },
        }
        self.armed = false;
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for XdpPacketBufInner {
    fn drop(&mut self) {
        if self.armed && self.storage.is_some() {
            self.reclaim();
        }
    }
}

/// Mutable AF_XDP packet buffer.
///
/// # Lifetime contract
///
/// A live buffer borrows socket/pool-owned UMEM through raw pointers but is an
/// owned, [`Send`] value with no lifetime tying it to its socket. **The socket
/// and pools that produced this buffer must outlive it** (including after it is
/// moved to another thread); dropping them first and then using — or dropping —
/// this buffer is undefined behavior. Debug builds turn that misuse into a clear
/// panic (see the module-level docs); release builds do not check it.
#[derive(Debug)]
#[repr(transparent)]
pub struct XdpPacketBufMut {
    inner: XdpPacketBufInner,
}

/// Frozen AF_XDP packet buffer.
///
/// Carries the same [lifetime contract](XdpPacketBufMut#lifetime-contract) as
/// [`XdpPacketBufMut`]: the owning socket/pool must outlive the buffer.
#[derive(Debug)]
#[repr(transparent)]
pub struct XdpPacketBuf {
    inner: XdpPacketBufInner,
}

// SAFETY: XDP buffers carry raw pointers into socket/pool-owned reclaim state
// and UMEM. They may be moved to worker threads for filling or dropped there;
// remote drops enter bounded MPSC reclaim queues. The socket and pools that
// created a buffer must outlive it, as documented in the module-level lifetime
// contract (and enforced in debug builds by the per-buffer owner-generation
// token). The `Cell` marker keeps buffers `!Sync`, so packet memory is not
// shared by reference across threads.
unsafe impl Send for XdpPacketBufMut {}

// SAFETY: same invariant as `XdpPacketBufMut`; freezing transfers ownership of
// the same backing frame into the immutable handle.
unsafe impl Send for XdpPacketBuf {}

impl XdpPacketBufMut {
    fn from_storage(
        storage: XdpStorage,
        ctx: NonNull<XdpBufCtx>,
        epoch: BufferEpoch,
        start: usize,
        end: usize,
    ) -> Self {
        let inner = XdpPacketBufInner {
            storage: Some(storage),
            ctx,
            epoch,
            armed: true,
            start,
            end,
            _not_sync: PhantomData,
        };
        debug_assert!(end <= inner.frame_capacity());
        Self { inner }
    }

    /// Returns packet bytes as a contiguous slice.
    ///
    /// AF_XDP ordering: this slice is read with plain (non-`volatile`) loads.
    /// That is sound because the wrapping `XdpPacketBufMut` was produced from
    /// an `RX` descriptor that the consumer cursor only published after an
    /// `Acquire` load of the kernel-side producer index in `RingConsumer::sync`,
    /// which on x86/ARM (and per the AF_XDP SPSC contract) is a memory
    /// fence sufficient to make the kernel's prior writes into this frame
    /// visible. Likewise, releasing the frame back to the FILL ring uses a
    /// `Release` store of the producer index, which fences our subsequent
    /// writes.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        let len = self.inner.end - self.inner.start;
        let ptr = self.inner.ptr();
        // SAFETY: start/end are maintained inside the backing frame.
        unsafe { slice::from_raw_parts(ptr.add(self.inner.start), len) }
    }

    /// Returns packet bytes as a mutable contiguous slice.
    ///
    /// See [`Self::as_slice`] for the AF_XDP memory-ordering rationale.
    #[must_use]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        let start = self.inner.start;
        let len = self.inner.end - start;
        let ptr = self.inner.mut_ptr();
        // SAFETY: start/end are maintained inside the backing frame, and &mut
        // self gives unique access to the packet bytes.
        unsafe { slice::from_raw_parts_mut(ptr.add(start), len) }
    }
}

impl XdpPacketBuf {
    /// Returns packet bytes as a contiguous slice.
    ///
    /// See [`XdpPacketBufMut::as_slice`] for the AF_XDP memory-ordering
    /// rationale shared by both frozen and mutable buffer types.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        let len = self.inner.end - self.inner.start;
        let ptr = self.inner.ptr();
        // SAFETY: start/end are maintained inside the backing frame.
        unsafe { slice::from_raw_parts(ptr.add(self.inner.start), len) }
    }

    pub(crate) fn prepare_tx_frame_with_header(&mut self, header: &[u8]) -> Option<XdpTxFrame> {
        let packet_len = self.len();
        if self.inner.storage.is_none() || !self.inner.is_umem() || header.len() > self.inner.start
        {
            return None;
        }
        let l2_start = self.inner.start - header.len();
        let len = header.len() + packet_len;
        if l2_start + len > self.inner.frame_capacity() {
            return None;
        }
        let dst = self.inner.mut_ptr();
        // SAFETY: l2_start was checked to be inside this frame.
        unsafe {
            ptr::copy_nonoverlapping(header.as_ptr(), dst.add(l2_start), header.len());
        }
        let frame_addr = self.inner.frame_addr()?;
        let desc_addr = frame_addr + l2_start as u64;
        Some(XdpTxFrame {
            desc_addr,
            len: len as u32,
        })
    }

    pub(crate) fn prepend(&mut self, bytes: &[u8]) -> Result<(), ReserveError> {
        prepend_to_inner(&mut self.inner, bytes)
    }

    pub(crate) fn trim_prefix(&mut self, len: usize) -> Result<(), BufferAccessError> {
        trim_prefix_from_inner(&mut self.inner, len)
    }

    pub(crate) fn mark_submitted(&mut self) {
        self.inner.disarm();
    }

    /// Marks this buffer as handed to the kernel's TX ring, disarming the
    /// `Drop` reclaim.
    ///
    /// For `Umem` storage the frame is reclaimed later through the COMPLETION
    /// ring, so dropping the reclaim handle here is correct. For the `Heap`
    /// fallback (test / unprivileged mode only) there is no completion ring, so
    /// the boxed frame is intentionally **not** returned to the heap pool — a
    /// submitted heap buffer is consumed. Production uses `Umem` storage; the
    /// heap path is scaffolding where leaking a pooled buffer per submit is
    /// acceptable.
    pub(crate) fn into_submitted(mut self) {
        self.mark_submitted();
    }
}

/// Descriptor facts for a prepared TX frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct XdpTxFrame {
    /// Descriptor address.
    pub(crate) desc_addr: u64,
    /// Descriptor length.
    pub(crate) len: u32,
}

impl PacketBuffer for XdpPacketBufMut {
    type Segments<'a> = XdpSegments<'a>;

    fn len(&self) -> usize {
        self.inner.end - self.inner.start
    }

    fn headroom(&self) -> usize {
        self.inner
            .start
            .checked_sub(self.inner.ctx().layout.l2_headroom())
            .expect("packet start >= l2_headroom by layout invariant")
    }

    fn tailroom(&self) -> usize {
        self.inner.frame_capacity().saturating_sub(self.inner.end)
    }

    fn layout(&self) -> &BufferLayout {
        &self.inner.ctx().layout
    }

    fn segments(&self) -> Self::Segments<'_> {
        if self.is_empty() {
            XdpSegments::empty()
        } else {
            XdpSegments::one(self.as_slice())
        }
    }

    fn read_at_exact(&self, offset: usize, dst: &mut [u8]) -> Result<(), BufferAccessError> {
        read_contiguous(self.as_slice(), offset, dst)
    }
}

impl PacketBuffer for XdpPacketBuf {
    type Segments<'a> = XdpSegments<'a>;

    fn len(&self) -> usize {
        self.inner.end - self.inner.start
    }

    fn headroom(&self) -> usize {
        self.inner
            .start
            .checked_sub(self.inner.ctx().layout.l2_headroom())
            .expect("packet start >= l2_headroom by layout invariant")
    }

    fn tailroom(&self) -> usize {
        self.inner.frame_capacity().saturating_sub(self.inner.end)
    }

    fn layout(&self) -> &BufferLayout {
        &self.inner.ctx().layout
    }

    fn segments(&self) -> Self::Segments<'_> {
        if self.is_empty() {
            XdpSegments::empty()
        } else {
            XdpSegments::one(self.as_slice())
        }
    }

    fn read_at_exact(&self, offset: usize, dst: &mut [u8]) -> Result<(), BufferAccessError> {
        read_contiguous(self.as_slice(), offset, dst)
    }
}

impl PacketBufferMut for XdpPacketBufMut {
    type Frozen = XdpPacketBuf;

    fn prepend(&mut self, bytes: &[u8]) -> Result<(), ReserveError> {
        prepend_to_inner(&mut self.inner, bytes)
    }

    fn extend_from_slice(&mut self, bytes: &[u8]) -> Result<(), BufferAccessError> {
        if bytes.len() > self.tailroom() {
            return Err(BufferAccessError::InsufficientTailroom {
                available: self.tailroom(),
                requested: bytes.len(),
            });
        }
        let end = self.inner.end;
        let dst = self.inner.mut_ptr();
        // SAFETY: tailroom prevalidation guarantees destination fits.
        unsafe {
            ptr::copy_nonoverlapping(bytes.as_ptr(), dst.add(end), bytes.len());
        }
        self.inner.end += bytes.len();
        Ok(())
    }

    fn trim_prefix(&mut self, len: usize) -> Result<(), BufferAccessError> {
        trim_prefix_from_inner(&mut self.inner, len)
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
        XdpPacketBuf { inner: self.inner }
    }
}

impl OwnedPacketBuffer for XdpPacketBuf {
    type Mutable = XdpPacketBufMut;

    fn into_mut(self) -> Self::Mutable {
        XdpPacketBufMut { inner: self.inner }
    }
}

fn prepend_to_inner(inner: &mut XdpPacketBufInner, bytes: &[u8]) -> Result<(), ReserveError> {
    let headroom = inner
        .start
        .checked_sub(inner.ctx().layout.l2_headroom())
        .expect("packet start >= l2_headroom by layout invariant");
    if bytes.len() > headroom {
        return Err(ReserveError::InsufficientHeadroom {
            available: headroom,
            requested: bytes.len(),
        });
    }
    let new_start = inner.start - bytes.len();
    let dst = inner.mut_ptr();
    // SAFETY: bounds checked above; new_start..old start lies inside the frame.
    unsafe {
        ptr::copy_nonoverlapping(bytes.as_ptr(), dst.add(new_start), bytes.len());
    }
    inner.start = new_start;
    Ok(())
}

fn trim_prefix_from_inner(
    inner: &mut XdpPacketBufInner,
    len: usize,
) -> Result<(), BufferAccessError> {
    let packet_len = inner.end - inner.start;
    if len > packet_len {
        return Err(BufferAccessError::OutOfBounds {
            offset: 0,
            len,
            packet_len,
        });
    }
    inner.start += len;
    Ok(())
}

fn read_contiguous(buffer: &[u8], offset: usize, dst: &mut [u8]) -> Result<(), BufferAccessError> {
    let end = offset
        .checked_add(dst.len())
        .ok_or(BufferAccessError::OutOfBounds {
            offset,
            len: dst.len(),
            packet_len: buffer.len(),
        })?;
    if end > buffer.len() {
        return Err(BufferAccessError::OutOfBounds {
            offset,
            len: dst.len(),
            packet_len: buffer.len(),
        });
    }
    dst.copy_from_slice(&buffer[offset..end]);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use fast_socket_rs::{HugePageSize, PacketBufferMut};

    use super::*;

    fn layout() -> BufferLayout {
        BufferLayout::with_headroom_and_tailroom(128, 64, 0)
            .with_l2_headroom(64)
            .with_alignment(NonZeroUsize::new(2048).unwrap())
            .with_fixed_chunk(2048, 2048)
    }

    fn umem() -> Rc<Umem> {
        Rc::new(Umem::new(2048, 2, HugePageSize::Default).unwrap())
    }

    fn assert_send<T: Send>() {}

    #[test]
    fn xdp_packet_buffers_are_send() {
        assert_send::<XdpPacketBufMut>();
        assert_send::<XdpPacketBuf>();
    }

    #[test]
    fn live_tx_pool_reuses_frame_on_drop() {
        let reclaim = FrameReclaim::new(vec![0]);
        let mut pool = XdpTxPool::live(layout(), umem(), Rc::clone(&reclaim));
        let mut packet = pool.allocate().expect("one frame available");
        packet.extend_from_slice(b"abc").unwrap();

        assert!(pool.allocate().is_none());
        drop(packet);
        assert!(pool.allocate().is_some());
    }

    #[test]
    fn live_tx_buffer_cross_thread_drop_reclaims_remotely() {
        let reclaim = FrameReclaim::new(vec![0]);
        let mut pool = XdpTxPool::live(layout(), umem(), Rc::clone(&reclaim));
        let mut packet = pool.allocate().expect("one frame available");
        packet.extend_from_slice(b"abc").unwrap();

        assert!(pool.allocate().is_none());
        std::thread::spawn(move || drop(packet)).join().unwrap();
        assert!(pool.allocate().is_some());
    }

    #[test]
    fn frozen_live_tx_buffer_cross_thread_drop_reclaims_remotely() {
        let reclaim = FrameReclaim::new(vec![0]);
        let mut pool = XdpTxPool::live(layout(), umem(), Rc::clone(&reclaim));
        let mut packet = pool.allocate().expect("one frame available");
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

    #[test]
    fn heap_tx_buffer_cross_thread_drop_reclaims_remotely() {
        let mut pool = XdpTxPool::with_heap_capacity(layout(), 1);
        let mut packet = pool.allocate().expect("one frame available");
        packet.extend_from_slice(b"abc").unwrap();

        assert!(pool.allocate().is_none());
        std::thread::spawn(move || drop(packet)).join().unwrap();
        assert!(pool.allocate().is_some());
    }

    #[test]
    fn frame_reclaim_pop_many_with_moves_requested_frames() {
        let reclaim = FrameReclaim::new(vec![0, 2048, 4096]);
        let mut frames = Vec::new();

        assert_eq!(reclaim.pop_many_with(2, |frame| frames.push(frame)), 2);
        assert_eq!(frames, vec![4096, 2048]);

        let mut remaining = Vec::new();
        reclaim.drain_into(&mut remaining);
        assert_eq!(remaining, vec![0]);
    }

    #[test]
    fn live_tx_pool_allocate_many_uses_one_reclaim_batch() {
        let umem = Rc::new(Umem::new(2048, 4, HugePageSize::Default).unwrap());
        let frames = vec![
            umem.frame_offset(0),
            umem.frame_offset(1),
            umem.frame_offset(2),
        ];
        let reclaim = FrameReclaim::new(frames.clone());
        let mut pool = XdpTxPool::live(layout(), Rc::clone(&umem), Rc::clone(&reclaim));
        let mut buffers = Vec::new();

        assert_eq!(pool.allocate_many(&mut buffers, 8), 3);
        assert_eq!(buffers.len(), 3);

        let mut remaining = Vec::new();
        reclaim.drain_into(&mut remaining);
        assert!(remaining.is_empty());

        drop(buffers);
        let mut reclaimed = Vec::new();
        reclaim.drain_into(&mut reclaimed);
        reclaimed.sort_unstable();
        assert_eq!(reclaimed, frames);
    }

    #[test]
    fn live_tx_pool_prepares_endpoint_batch_directly() {
        let umem = Rc::new(Umem::new(2048, 4, HugePageSize::Default).unwrap());
        let reclaim = FrameReclaim::new(vec![umem.frame_offset(0), umem.frame_offset(1)]);
        let mut pool = XdpTxPool::live(layout(), Rc::clone(&umem), Rc::clone(&reclaim));
        let header = [0xab; 42];
        let payload_capacity = 8;
        let payload_lens = [3usize, 5];
        let mut descs = Vec::new();

        let batch = pool
            .prepare_endpoint_batch(
                &mut descs,
                &header,
                14,
                payload_capacity,
                2,
                |index, _header, payload| {
                    let payload_len = payload_lens[index];
                    payload[..payload_len].fill(index as u8 + 1);
                    Ok(payload_len)
                },
            )
            .expect("live pool can prepare direct endpoint batch");

        assert_eq!(batch.prepared, 2);
        assert_eq!(batch.tx_bytes, 64);
        assert_eq!(descs.len(), 2);
        for (index, desc) in descs.iter().enumerate() {
            let payload_len = payload_lens[index];
            assert_eq!(desc.len as usize, header.len() + payload_len);
            assert_eq!(umem.slice_at(desc.addr, header.len()), &header);
            assert_eq!(
                umem.slice_at(desc.addr + header.len() as u64, payload_len),
                vec![index as u8 + 1; payload_len]
            );
        }

        assert!(pool.allocate().is_none());
    }

    #[test]
    fn submitted_tx_frame_reclaims_only_on_completion() {
        let reclaim = FrameReclaim::new(vec![0]);
        let mut pool = XdpTxPool::live(layout(), umem(), Rc::clone(&reclaim));
        let mut packet = pool.allocate().expect("one frame available");
        packet.extend_from_slice(b"abc").unwrap();
        let mut packet = packet.freeze();
        let header = [0u8; 14];
        let frame = packet
            .prepare_tx_frame_with_header(&header)
            .expect("live frame can be prepared");
        packet.into_submitted();

        assert!(pool.allocate().is_none());
        pool.reclaim_completed(frame.desc_addr);
        assert!(pool.allocate().is_some());
    }

    #[test]
    fn live_rx_buffer_drop_returns_frame_to_reclaim_list() {
        let reclaim = FrameReclaim::new(Vec::new());
        let pool = XdpRxPool::live(layout(), umem(), Rc::clone(&reclaim));
        let packet = pool
            .wrap_rx_frame(0, layout().data_offset(), 16)
            .expect("rx frame wraps");
        drop(packet);

        let mut reclaimed = Vec::new();
        reclaim.drain_into(&mut reclaimed);
        assert_eq!(reclaimed, vec![0]);
    }

    #[test]
    fn live_rx_buffer_cross_thread_drop_reclaims_remotely() {
        let reclaim = FrameReclaim::new(Vec::with_capacity(1));
        let pool = XdpRxPool::live(layout(), umem(), Rc::clone(&reclaim));
        let packet = pool
            .wrap_rx_frame(0, layout().data_offset(), 16)
            .expect("rx frame wraps");

        std::thread::spawn(move || drop(packet)).join().unwrap();

        let mut reclaimed = Vec::new();
        reclaim.drain_into(&mut reclaimed);
        assert_eq!(reclaimed, vec![0]);
    }

    #[test]
    fn live_rx_buffer_wraps_desc_offset_without_consuming_l2_headroom() {
        let reclaim = FrameReclaim::new(Vec::new());
        let pool = XdpRxPool::live(layout(), umem(), Rc::clone(&reclaim));
        let packet = pool.wrap_rx_frame(64, 14, 16).expect("rx frame wraps");

        assert_eq!(packet.headroom(), 14);
        drop(packet);

        let mut reclaimed = Vec::new();
        reclaim.drain_into(&mut reclaimed);
        assert_eq!(reclaimed, vec![0]);
    }

    /// Dropping the owning pool/UMEM while a live buffer is still alive is the
    /// documented UB case. When the guard is active (debug builds or the
    /// `buffer-guard` feature), the owner-generation check must turn a subsequent
    /// buffer access into a clear panic rather than a dangling read. With the
    /// guard disabled there is no check, so this test only runs when guarded.
    #[cfg(any(debug_assertions, feature = "buffer-guard"))]
    #[test]
    fn use_after_owner_drop_panics_when_guarded() {
        let reclaim = FrameReclaim::new(vec![0]);
        let pool = XdpRxPool::live(layout(), umem(), Rc::clone(&reclaim));
        let packet = pool
            .wrap_rx_frame(0, layout().data_offset(), 16)
            .expect("rx frame wraps");

        // Tear down every owner of the UMEM/reclaim out from under the buffer.
        drop(pool);
        drop(reclaim);

        // Touching the buffer now would read freed UMEM in release; the debug
        // guard must catch it instead.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = packet.as_slice();
        }));
        assert!(
            result.is_err(),
            "use-after-owner-drop must be detected in debug builds"
        );

        // The buffer is still alive; its own `Drop` would (correctly) assert
        // too, so forget it to avoid a double panic aborting the test.
        std::mem::forget(packet);
    }
}
