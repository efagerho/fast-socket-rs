//! Slab-backed packet buffers for the OS UDP backend.

use std::cell::UnsafeCell;
use std::fmt;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::rc::Rc;

use fast_socket_rs::{
    BufferAccessError, BufferLayout, BufferPool, OwnedPacketBuffer, PacketBuffer, PacketBufferMut,
    ReserveError, Segment,
};

const SLAB_SIZE: usize = 64;
/// Upper bound on total backing buffers a single pool will hold across all
/// slab grows. 64 KiB × 2 KiB ≈ 128 MiB of resident memory per pool, which
/// dwarfs any reasonable socket workload while still being large enough that
/// well-behaved callers never see allocation failures.
const MAX_POOL_BUFFERS: usize = 64 * 1024;

#[derive(Debug)]
struct OsPoolInner {
    free: UnsafeCell<Vec<Vec<u8>>>,
    /// Total number of backing buffers issued by this pool (free + in-flight).
    /// Used to cap growth so a misbehaving caller cannot exhaust system
    /// memory by holding allocations forever.
    allocated: UnsafeCell<usize>,
    /// Carry an explicit `!Send + !Sync` marker so that any future refactor
    /// that swaps `Rc<OsPoolInner>` for `Arc<OsPoolInner>` (or removes the
    /// `Rc` entirely) still surfaces the thread-locality invariant at the
    /// type level. The `UnsafeCell` here is only safe under the
    /// "single-thread owns the pool" rule documented on [`OsBufferPool`].
    _not_sync: PhantomData<*const ()>,
}

impl OsPoolInner {
    fn new() -> Self {
        Self {
            free: UnsafeCell::new(Vec::new()),
            allocated: UnsafeCell::new(0),
            _not_sync: PhantomData,
        }
    }

    fn pop(&self) -> Option<Vec<u8>> {
        // The live OS socket and its pools are intentionally single-threaded.
        unsafe { &mut *self.free.get() }.pop()
    }

    fn push(&self, storage: Vec<u8>) {
        // The live OS socket and its pools are intentionally single-threaded.
        unsafe { &mut *self.free.get() }.push(storage);
    }

    /// Allocates another slab of backing buffers, capped at
    /// [`MAX_POOL_BUFFERS`] total. Returns the number of buffers added so
    /// callers can detect exhaustion.
    fn grow(&self, layout: BufferLayout) -> usize {
        // SAFETY: owner-thread only; see the `!Sync` marker.
        let allocated = unsafe { &mut *self.allocated.get() };
        let remaining = MAX_POOL_BUFFERS.saturating_sub(*allocated);
        if remaining == 0 {
            return 0;
        }
        let chunk = SLAB_SIZE.min(remaining);
        let allocation_len = layout.allocation_len();
        // SAFETY: owner-thread only; see the `!Sync` marker.
        let free = unsafe { &mut *self.free.get() };
        free.reserve(chunk);
        for _ in 0..chunk {
            free.push(vec![0; allocation_len]);
        }
        *allocated += chunk;
        chunk
    }
}

/// Slab-backed buffer pool used by [`crate::OsUdpSocket`].
///
/// The pool is queue-local and intentionally not thread-safe. Returning packet
/// storage to the pool uses non-atomic reference counts and an owner-thread free
/// list, so the steady path avoids allocator churn without introducing
/// cross-core synchronization.
#[derive(Clone)]
pub struct OsBufferPool {
    layout: BufferLayout,
    inner: Rc<OsPoolInner>,
}

impl OsBufferPool {
    /// Creates a slab-backed pool for `layout`.
    #[must_use]
    pub fn new(layout: BufferLayout) -> Self {
        Self {
            layout: layout.with_max_segments(1),
            inner: Rc::new(OsPoolInner::new()),
        }
    }
}

impl fmt::Debug for OsBufferPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OsBufferPool")
            .field("layout", &self.layout)
            .finish_non_exhaustive()
    }
}

impl BufferPool for OsBufferPool {
    type Buffer = OsPacketBufMut;

    fn layout(&self) -> &BufferLayout {
        &self.layout
    }

    fn allocate(&mut self) -> Option<Self::Buffer> {
        let mut storage = match self.inner.pop() {
            Some(storage) => storage,
            None => {
                if self.inner.grow(self.layout) == 0 {
                    // Pool is at its hard cap and every buffer is currently
                    // in flight. Surface as `None` so callers see allocation
                    // back-pressure instead of OOMing the host.
                    return None;
                }
                self.inner.pop()?
            }
        };

        let allocation_len = self.layout.allocation_len();
        if storage.len() != allocation_len {
            storage.resize(allocation_len, 0);
        }

        Some(OsPacketBufMut {
            storage,
            start: self.layout.data_offset(),
            end: self.layout.data_offset(),
            layout: self.layout,
            pool: Rc::clone(&self.inner),
        })
    }
}

/// Mutable OS UDP packet buffer.
pub struct OsPacketBufMut {
    storage: Vec<u8>,
    start: usize,
    end: usize,
    layout: BufferLayout,
    pool: Rc<OsPoolInner>,
}

impl fmt::Debug for OsPacketBufMut {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OsPacketBufMut")
            .field("len", &self.len())
            .field("headroom", &self.headroom())
            .field("tailroom", &self.tailroom())
            .field("layout", &self.layout)
            .finish_non_exhaustive()
    }
}

impl OsPacketBufMut {
    /// Returns the current packet bytes as a contiguous slice.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.storage[self.start..self.end]
    }

    /// Returns the current packet bytes as a mutable contiguous slice.
    #[must_use]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.storage[self.start..self.end]
    }

    pub(crate) fn data_ptr(&mut self) -> *mut u8 {
        unsafe { self.storage.as_mut_ptr().add(self.layout.data_offset()) }
    }

    pub(crate) fn data_capacity(&self) -> usize {
        self.layout.payload_capacity()
    }

    pub(crate) fn set_received_len(&mut self, len: usize) -> Result<(), BufferAccessError> {
        if len > self.layout.payload_capacity() {
            return Err(BufferAccessError::InsufficientTailroom {
                available: self.layout.payload_capacity(),
                requested: len,
            });
        }
        self.start = self.layout.data_offset();
        self.end = self.start + len;
        Ok(())
    }
}

impl Drop for OsPacketBufMut {
    fn drop(&mut self) {
        let storage = std::mem::take(&mut self.storage);
        self.pool.push(storage);
    }
}

/// Immutable OS UDP packet buffer.
pub struct OsPacketBuf {
    storage: Vec<u8>,
    start: usize,
    end: usize,
    layout: BufferLayout,
    pool: Rc<OsPoolInner>,
}

impl fmt::Debug for OsPacketBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OsPacketBuf")
            .field("len", &self.len())
            .field("headroom", &self.headroom())
            .field("tailroom", &self.tailroom())
            .field("layout", &self.layout)
            .finish_non_exhaustive()
    }
}

impl OsPacketBuf {
    /// Returns the packet bytes as a contiguous slice.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.storage[self.start..self.end]
    }
}

impl Drop for OsPacketBuf {
    fn drop(&mut self) {
        let storage = std::mem::take(&mut self.storage);
        self.pool.push(storage);
    }
}

impl OwnedPacketBuffer for OsPacketBuf {
    type Mutable = OsPacketBufMut;

    fn into_mut(self) -> Self::Mutable {
        let this = ManuallyDrop::new(self);
        OsPacketBufMut {
            storage: unsafe { std::ptr::read(&this.storage) },
            start: this.start,
            end: this.end,
            layout: this.layout,
            pool: unsafe { std::ptr::read(&this.pool) },
        }
    }
}

/// The `PacketBuffer` read surface is identical for the immutable and mutable
/// OS buffers (same `start`/`end`/`storage`/`layout` fields and `as_slice`), so
/// emit it once for each type instead of duplicating the six methods.
macro_rules! impl_os_packet_buffer {
    ($ty:ty) => {
        impl PacketBuffer for $ty {
            type Segments<'a> = std::option::IntoIter<Segment<'a>>;

            fn len(&self) -> usize {
                self.end - self.start
            }

            fn headroom(&self) -> usize {
                self.start
                    .checked_sub(self.layout.l2_headroom())
                    .expect("packet start is above l2 headroom")
            }

            fn tailroom(&self) -> usize {
                self.storage.len() - self.end
            }

            fn layout(&self) -> &BufferLayout {
                &self.layout
            }

            fn segments(&self) -> Self::Segments<'_> {
                (!self.is_empty()).then_some(self.as_slice()).into_iter()
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

    fn prepend(&mut self, bytes: &[u8]) -> Result<(), ReserveError> {
        if bytes.len() > self.headroom() {
            return Err(ReserveError::InsufficientHeadroom {
                available: self.headroom(),
                requested: bytes.len(),
            });
        }
        let new_start = self.start - bytes.len();
        self.storage[new_start..self.start].copy_from_slice(bytes);
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

        let next_end = self.end + bytes.len();
        self.storage[self.end..next_end].copy_from_slice(bytes);
        self.end = next_end;
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

    fn freeze(self) -> Self::Frozen {
        let this = ManuallyDrop::new(self);
        OsPacketBuf {
            storage: unsafe { std::ptr::read(&this.storage) },
            start: this.start,
            end: this.end,
            layout: this.layout,
            pool: unsafe { std::ptr::read(&this.pool) },
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
