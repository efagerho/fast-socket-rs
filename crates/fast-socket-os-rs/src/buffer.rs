//! Slab-backed packet buffers for the OS UDP backend.

use std::fmt;
use std::mem::ManuallyDrop;
use std::sync::{Arc, Mutex, MutexGuard};

use fast_socket_rs::{
    BufferAccessError, BufferLayout, BufferPool, OwnedPacketBuffer, PacketBuffer, PacketBufferMut,
    ReserveError, Segment,
};

const SLAB_SIZE: usize = 64;

#[derive(Debug)]
struct OsPoolInner {
    // TODO: Replace this with a more performant implementation later.
    state: Mutex<OsPoolState>,
    max_buffers: usize,
}

#[derive(Debug)]
struct OsPoolState {
    free: Vec<Vec<u8>>,
    /// Total number of backing buffers issued by this pool (free + in-flight).
    /// Used to cap growth so a misbehaving caller cannot exhaust system
    /// memory by holding allocations forever.
    allocated: usize,
}

impl OsPoolInner {
    fn new(max_buffers: usize) -> Self {
        Self {
            state: Mutex::new(OsPoolState {
                free: Vec::new(),
                allocated: 0,
            }),
            max_buffers,
        }
    }

    fn state(&self) -> MutexGuard<'_, OsPoolState> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    fn pop(&self) -> Option<Vec<u8>> {
        self.state().free.pop()
    }

    fn push(&self, storage: Vec<u8>) {
        self.state().free.push(storage);
    }

    /// Allocates another slab of backing buffers, capped at `max_buffers`
    /// total. Returns the number of buffers added so callers can detect
    /// exhaustion.
    fn grow(&self, layout: BufferLayout) -> usize {
        let mut state = self.state();
        let remaining = self.max_buffers.saturating_sub(state.allocated);
        if remaining == 0 {
            return 0;
        }
        let chunk = SLAB_SIZE.min(remaining);
        let allocation_len = layout.allocation_len();
        state.free.reserve(chunk);
        for _ in 0..chunk {
            state.free.push(vec![0; allocation_len]);
        }
        state.allocated += chunk;
        chunk
    }
}

/// Slab-backed buffer pool used by [`crate::OsUdpSocket`].
///
/// Packet buffers are [`Send`], so storage can be returned to the pool from a
/// different worker thread. The pool keeps its free list behind shared
/// synchronized state while the live socket itself remains single-thread owned.
#[derive(Clone)]
pub struct OsBufferPool {
    layout: BufferLayout,
    inner: Arc<OsPoolInner>,
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
        Self {
            layout: layout.with_max_segments(1),
            inner: Arc::new(OsPoolInner::new(max_buffers)),
        }
    }
}

impl fmt::Debug for OsBufferPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OsBufferPool")
            .field("layout", &self.layout)
            .field("max_buffers", &self.inner.max_buffers)
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
                    // Pool is at its cap with no free buffer available. Surface
                    // this as back-pressure instead of OOMing the host.
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
            pool: Arc::clone(&self.inner),
        })
    }
}

/// Mutable OS UDP packet buffer.
pub struct OsPacketBufMut {
    storage: Vec<u8>,
    start: usize,
    end: usize,
    layout: BufferLayout,
    pool: Arc<OsPoolInner>,
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
    pool: Arc<OsPoolInner>,
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
}
