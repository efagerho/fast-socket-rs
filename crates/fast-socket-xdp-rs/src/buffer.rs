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
    BufferAccessError, BufferLayout, BufferPool, OwnedPacketBuffer, PacketBuffer, PacketBufferMut,
    ReserveError, Segment,
};

use crate::umem::Umem;

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
}

#[derive(Debug)]
pub(crate) struct FrameReclaim {
    owner: ThreadId,
    free: UnsafeCell<Vec<u64>>,
    remote: MpscQueue<u64>,
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
        })
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
        let remote_capacity = frames.capacity();
        Rc::new(Self {
            owner: thread::current().id(),
            free: UnsafeCell::new(frames),
            remote: MpscQueue::new(remote_capacity),
        })
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
    // decrements as it drains. The hot drain path loads this with Relaxed and
    // skips touching `inner` when zero, so single-threaded workloads never pay
    // the cross-thread cache-line cost on `inner.pop`.
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
        self.len.load(Ordering::Relaxed) == 0
    }

    fn drain_into(&self, out: &mut Vec<T>) {
        while let Some(value) = self.inner.pop() {
            out.push(value);
            self.len.fetch_sub(1, Ordering::Relaxed);
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
    layout: BufferLayout,
    heap: Rc<HeapReclaim>,
    live: Option<XdpLivePool>,
}

/// Transmit pool for AF_XDP IP packet sockets.
#[derive(Debug)]
pub struct XdpTxPool {
    layout: BufferLayout,
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
        Self {
            layout,
            heap: HeapReclaim::new(layout, count),
            live: None,
        }
    }

    pub(crate) fn live(layout: BufferLayout, umem: Rc<Umem>, reclaim: Rc<FrameReclaim>) -> Self {
        Self {
            layout,
            heap: HeapReclaim::new(layout, 0),
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
        if start < self.layout.l2_headroom() || start.checked_add(len)? > frame_size as usize {
            return None;
        }
        // The pool-owned `Rc`s keep these allocations alive. Returned buffers
        // may cross threads, but the socket/pool must outlive them all; see the
        // module-level lifetime contract.
        let umem_ptr = NonNull::from(live.umem.as_ref());
        let reclaim_ptr = NonNull::from(live.reclaim.as_ref());
        Some(XdpPacketBufMut::from_storage(
            XdpStorage::Umem {
                umem: umem_ptr,
                frame_addr,
                frame_size: live.umem.frame_size() as usize,
                reclaim: Some(reclaim_ptr),
            },
            self.layout,
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
        Self {
            layout,
            heap: HeapReclaim::new(layout, count),
            live: None,
        }
    }

    pub(crate) fn live(layout: BufferLayout, umem: Rc<Umem>, reclaim: Rc<FrameReclaim>) -> Self {
        Self {
            layout,
            heap: HeapReclaim::new(layout, 0),
            live: Some(XdpLivePool { umem, reclaim }),
        }
    }

    pub(crate) fn reclaim_completed_frame(&mut self, frame_addr: u64) {
        if let Some(live) = &self.live {
            debug_assert!(live.umem.contains_frame_addr(frame_addr));
            live.reclaim.push_local(frame_addr);
        }
    }

    pub(crate) fn allocate_many(&mut self, out: &mut Vec<XdpPacketBufMut>, max: usize) -> usize {
        if max == 0 {
            return 0;
        }

        if let Some(live) = &self.live {
            // The pool-owned `Rc`s keep these allocations alive. Returned
            // buffers may cross threads, but the socket/pool must outlive them
            // all; see the module-level lifetime contract.
            let umem_ptr = NonNull::from(live.umem.as_ref());
            let reclaim_ptr = NonNull::from(live.reclaim.as_ref());
            let frame_size = live.umem.frame_size() as usize;
            let layout = self.layout;
            out.reserve(max);
            return live.reclaim.pop_many_with(max, |frame_addr| {
                out.push(XdpPacketBufMut::from_storage(
                    XdpStorage::Umem {
                        umem: umem_ptr,
                        frame_addr,
                        frame_size,
                        reclaim: Some(reclaim_ptr),
                    },
                    layout,
                    layout.data_offset(),
                    layout.data_offset(),
                ));
            });
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

    #[cfg(test)]
    pub(crate) fn reclaim_completed(&mut self, desc_addr: u64) {
        if let Some(live) = &self.live {
            let frame_size = live.umem.frame_size() as u64;
            let frame_addr = desc_addr - (desc_addr % frame_size);
            live.reclaim.push_local(frame_addr);
        }
    }
}

impl BufferPool for XdpRxPool {
    type Buffer = XdpPacketBufMut;

    fn layout(&self) -> &BufferLayout {
        &self.layout
    }

    fn allocate(&mut self) -> Option<Self::Buffer> {
        let frame = self.heap.pop()?;
        Some(XdpPacketBufMut::from_storage(
            XdpStorage::Heap {
                frame,
                reclaim: Some(NonNull::from(self.heap.as_ref())),
            },
            self.layout,
            self.layout.data_offset(),
            self.layout.data_offset(),
        ))
    }
}

impl BufferPool for XdpTxPool {
    type Buffer = XdpPacketBufMut;

    fn layout(&self) -> &BufferLayout {
        &self.layout
    }

    fn allocate(&mut self) -> Option<Self::Buffer> {
        if let Some(live) = &self.live {
            let frame_addr = live.reclaim.pop()?;
            // See allocate_many: the socket/pool must outlive all buffers.
            let umem_ptr = NonNull::from(live.umem.as_ref());
            let reclaim_ptr = NonNull::from(live.reclaim.as_ref());
            return Some(XdpPacketBufMut::from_storage(
                XdpStorage::Umem {
                    umem: umem_ptr,
                    frame_addr,
                    frame_size: live.umem.frame_size() as usize,
                    reclaim: Some(reclaim_ptr),
                },
                self.layout,
                self.layout.data_offset(),
                self.layout.data_offset(),
            ));
        }

        let frame = self.heap.pop()?;
        Some(XdpPacketBufMut::from_storage(
            XdpStorage::Heap {
                frame,
                reclaim: Some(NonNull::from(self.heap.as_ref())),
            },
            self.layout,
            self.layout.data_offset(),
            self.layout.data_offset(),
        ))
    }
}

enum XdpStorage {
    Heap {
        frame: Box<[u8]>,
        reclaim: Option<NonNull<HeapReclaim>>,
    },
    Umem {
        // SAFETY invariant: `umem` and `reclaim` point at allocations owned by
        // the socket/pool that handed this storage out. Buffers are `Send` and
        // may be filled or dropped on worker threads, so the owner socket/pool
        // must outlive every outstanding buffer. Cross-thread drops push into
        // the reclaim object's remote MPSC queue; owner-thread drops use its
        // local free list.
        umem: NonNull<Umem>,
        frame_addr: u64,
        frame_size: usize,
        reclaim: Option<NonNull<FrameReclaim>>,
    },
}

impl fmt::Debug for XdpStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Heap { frame, .. } => f
                .debug_struct("Heap")
                .field("len", &frame.len())
                .finish_non_exhaustive(),
            Self::Umem {
                frame_addr,
                frame_size,
                ..
            } => f
                .debug_struct("Umem")
                .field("frame_addr", frame_addr)
                .field("frame_size", frame_size)
                .finish_non_exhaustive(),
        }
    }
}

impl XdpStorage {
    fn ptr(&self) -> *const u8 {
        match self {
            Self::Heap { frame, .. } => frame.as_ptr(),
            Self::Umem {
                umem, frame_addr, ..
            } => {
                // SAFETY: the socket/pool keeps the UMEM allocation alive for
                // the lifetime of this storage (see XdpStorage::Umem
                // invariant), and `frame_addr` is produced from its frame
                // table.
                unsafe { umem.as_ref().as_ptr().add(*frame_addr as usize) }
            }
        }
    }

    fn mut_ptr(&mut self) -> *mut u8 {
        match self {
            Self::Heap { frame, .. } => frame.as_mut_ptr(),
            Self::Umem {
                umem, frame_addr, ..
            } => {
                // SAFETY: same UMEM-liveness invariant as `ptr`. Even when the
                // buffer moves to another thread, Rust ownership gives that
                // thread exclusive access to this packet frame.
                unsafe { (umem.as_ref().as_ptr() as *mut u8).add(*frame_addr as usize) }
            }
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Heap { frame, .. } => frame.len(),
            Self::Umem { frame_size, .. } => *frame_size,
        }
    }

    fn reclaim(&mut self) {
        match self {
            Self::Heap { frame, reclaim } => {
                if let Some(reclaim) = reclaim.take() {
                    let empty = Vec::new().into_boxed_slice();
                    let frame = mem::replace(frame, empty);
                    // SAFETY: same socket/pool-outlives-buffer invariant as
                    // live UMEM storage.
                    unsafe { reclaim.as_ref() }.push(frame);
                }
            }
            Self::Umem {
                frame_addr,
                reclaim,
                ..
            } => {
                if let Some(reclaim) = reclaim.take() {
                    // SAFETY: the socket/pool keeps the FrameReclaim
                    // allocation alive for the lifetime of this storage (see
                    // XdpStorage::Umem invariant). Remote-thread drops use the
                    // reclaim object's bounded MPSC queue.
                    unsafe { reclaim.as_ref() }.push(*frame_addr);
                }
            }
        }
    }

    fn is_umem(&self) -> bool {
        matches!(self, Self::Umem { .. })
    }

    fn frame_addr(&self) -> Option<u64> {
        match self {
            Self::Umem { frame_addr, .. } => Some(*frame_addr),
            Self::Heap { .. } => None,
        }
    }
}

/// Mutable AF_XDP packet buffer.
#[derive(Debug)]
pub struct XdpPacketBufMut {
    storage: Option<XdpStorage>,
    layout: BufferLayout,
    start: usize,
    end: usize,
    _not_sync: PhantomData<Cell<()>>,
}

/// Frozen AF_XDP packet buffer.
#[derive(Debug)]
pub struct XdpPacketBuf {
    storage: Option<XdpStorage>,
    layout: BufferLayout,
    start: usize,
    end: usize,
    _not_sync: PhantomData<Cell<()>>,
}

// SAFETY: XDP buffers carry raw pointers into socket/pool-owned reclaim state
// and UMEM. They may be moved to worker threads for filling or dropped there;
// remote drops enter bounded MPSC reclaim queues. The socket and pools that
// created a buffer must outlive it, as documented in the module-level lifetime
// contract. The `Cell` marker keeps buffers `!Sync`, so packet memory is not
// shared by reference across threads.
unsafe impl Send for XdpPacketBufMut {}

// SAFETY: same invariant as `XdpPacketBufMut`; freezing transfers ownership of
// the same backing frame into the immutable handle.
unsafe impl Send for XdpPacketBuf {}

impl Drop for XdpPacketBufMut {
    fn drop(&mut self) {
        if let Some(storage) = self.storage.as_mut() {
            storage.reclaim();
        }
    }
}

impl Drop for XdpPacketBuf {
    fn drop(&mut self) {
        if let Some(storage) = self.storage.as_mut() {
            storage.reclaim();
        }
    }
}

impl XdpPacketBufMut {
    fn from_storage(storage: XdpStorage, layout: BufferLayout, start: usize, end: usize) -> Self {
        debug_assert!(end <= storage.len());
        Self {
            storage: Some(storage),
            layout,
            start,
            end,
            _not_sync: PhantomData,
        }
    }

    /// Returns packet bytes as a contiguous slice.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        let storage = self.storage.as_ref().expect("buffer storage is present");
        // SAFETY: start/end are maintained inside the backing frame.
        unsafe { slice::from_raw_parts(storage.ptr().add(self.start), self.end - self.start) }
    }

    /// Returns packet bytes as a mutable contiguous slice.
    #[must_use]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        let storage = self.storage.as_mut().expect("buffer storage is present");
        // SAFETY: start/end are maintained inside the backing frame, and &mut
        // self gives unique access to the packet bytes.
        unsafe {
            slice::from_raw_parts_mut(storage.mut_ptr().add(self.start), self.end - self.start)
        }
    }
}

impl XdpPacketBuf {
    /// Returns packet bytes as a contiguous slice.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        let storage = self.storage.as_ref().expect("buffer storage is present");
        // SAFETY: start/end are maintained inside the backing frame.
        unsafe { slice::from_raw_parts(storage.ptr().add(self.start), self.end - self.start) }
    }

    pub(crate) fn prepare_l2(&mut self, header: &[u8]) -> Option<XdpTxFrame> {
        let packet_len = self.len();
        let storage = self.storage.as_mut()?;
        if !storage.is_umem() || header.len() > self.start {
            return None;
        }
        let l2_start = self.start - header.len();
        let len = header.len() + packet_len;
        if l2_start + len > storage.len() {
            return None;
        }
        // SAFETY: l2_start was checked to be inside this frame.
        unsafe {
            ptr::copy_nonoverlapping(
                header.as_ptr(),
                storage.mut_ptr().add(l2_start),
                header.len(),
            );
        }
        let frame_addr = storage.frame_addr()?;
        let desc_addr = frame_addr + l2_start as u64;
        Some(XdpTxFrame {
            desc_addr,
            len: len as u32,
        })
    }

    pub(crate) fn into_submitted(mut self) {
        if let Some(storage) = self.storage.as_mut() {
            match storage {
                XdpStorage::Heap { reclaim, .. } => {
                    let _ = reclaim.take();
                }
                XdpStorage::Umem { reclaim, .. } => {
                    let _ = reclaim.take();
                }
            }
        }
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
        self.end - self.start
    }

    fn headroom(&self) -> usize {
        self.start
            .checked_sub(self.layout.l2_headroom())
            .expect("packet start >= l2_headroom by layout invariant")
    }

    fn tailroom(&self) -> usize {
        self.storage
            .as_ref()
            .expect("buffer storage is present")
            .len()
            .saturating_sub(self.end)
    }

    fn layout(&self) -> &BufferLayout {
        &self.layout
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
        self.end - self.start
    }

    fn headroom(&self) -> usize {
        self.start
            .checked_sub(self.layout.l2_headroom())
            .expect("packet start >= l2_headroom by layout invariant")
    }

    fn tailroom(&self) -> usize {
        self.storage
            .as_ref()
            .expect("buffer storage is present")
            .len()
            .saturating_sub(self.end)
    }

    fn layout(&self) -> &BufferLayout {
        &self.layout
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
        if bytes.len() > self.headroom() {
            return Err(ReserveError::InsufficientHeadroom {
                available: self.headroom(),
                requested: bytes.len(),
            });
        }
        let storage = self.storage.as_mut().expect("buffer storage is present");
        let new_start = self.start - bytes.len();
        // SAFETY: bounds checked above; new_start..self.start lies inside the frame.
        unsafe {
            ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                storage.mut_ptr().add(new_start),
                bytes.len(),
            );
        }
        self.start = new_start;
        Ok(())
    }

    fn extend_from_slice(&mut self, bytes: &[u8]) -> Result<(), BufferAccessError> {
        if bytes.len() > self.tailroom() {
            return Err(BufferAccessError::InsufficientTailroom {
                available: self.tailroom(),
                requested: bytes.len(),
            });
        }
        let storage = self.storage.as_mut().expect("buffer storage is present");
        // SAFETY: tailroom prevalidation guarantees destination fits.
        unsafe {
            ptr::copy_nonoverlapping(bytes.as_ptr(), storage.mut_ptr().add(self.end), bytes.len());
        }
        self.end += bytes.len();
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
        self.start += len;
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
        self.end -= len;
        Ok(())
    }

    fn freeze(mut self) -> Self::Frozen {
        XdpPacketBuf {
            storage: self.storage.take(),
            layout: self.layout,
            start: self.start,
            end: self.end,
            _not_sync: PhantomData,
        }
    }
}

impl OwnedPacketBuffer for XdpPacketBuf {
    type Mutable = XdpPacketBufMut;

    fn into_mut(mut self) -> Self::Mutable {
        XdpPacketBufMut {
            storage: self.storage.take(),
            layout: self.layout,
            start: self.start,
            end: self.end,
            _not_sync: PhantomData,
        }
    }
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

    use fast_socket_rs::{BufferPool, HugePageSize, PacketBufferMut};

    use super::*;

    fn layout() -> BufferLayout {
        BufferLayout::with_headroom_and_tailroom(128, 64, 0)
            .with_l2_headroom(64)
            .with_alignment(NonZeroUsize::new(2048).unwrap())
            .with_fixed_chunk(2048, 2048)
            .unwrap()
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
    fn submitted_tx_frame_reclaims_only_on_completion() {
        let reclaim = FrameReclaim::new(vec![0]);
        let mut pool = XdpTxPool::live(layout(), umem(), Rc::clone(&reclaim));
        let mut packet = pool.allocate().expect("one frame available");
        packet.extend_from_slice(b"abc").unwrap();
        let mut packet = packet.freeze();
        let header = [0u8; 14];
        let frame = packet
            .prepare_l2(&header)
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
}
