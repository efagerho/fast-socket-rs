//! AF_XDP ring mmap and cursor helpers.

use std::io;
use std::marker::PhantomData;
use std::mem;
use std::os::fd::RawFd;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, Ordering};

const XDP_RING_NEED_WAKEUP: u32 = 1;

/// AF_XDP descriptor matching Linux `struct xdp_desc`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct XdpDesc {
    /// UMEM address.
    pub addr: u64,
    /// Descriptor length.
    pub len: u32,
    /// Descriptor options.
    pub options: u32,
}

/// Reserved descriptor indexes in a ring cursor.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RingRange {
    /// First reserved absolute ring index.
    pub start: u32,
    /// Number of reserved indexes.
    pub count: u32,
}

impl RingRange {
    /// Returns true when this range contains no descriptors.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.count == 0
    }
}

/// Memory mapping for an AF_XDP ring.
#[derive(Debug)]
pub struct RingMmap<T> {
    ptr: NonNull<u8>,
    len: usize,
    /// Producer index.
    pub producer: *mut AtomicU32,
    /// Consumer index.
    pub consumer: *mut AtomicU32,
    /// Descriptor array.
    pub desc: *mut T,
    /// Ring flags word.
    pub flags: *mut AtomicU32,
    _marker: PhantomData<T>,
}

// SAFETY: mappings move with the owning queue object. Concurrent ring access is
// a backend invariant, not provided by this type.
unsafe impl<T> Send for RingMmap<T> {}

impl<T> RingMmap<T> {
    /// Returns true when the kernel asks userspace to kick this ring.
    #[must_use]
    pub fn needs_wakeup(&self) -> bool {
        // SAFETY: flags points into the live ring mmap owned by self.
        unsafe { (*self.flags).load(Ordering::Acquire) & XDP_RING_NEED_WAKEUP != 0 }
    }
}

impl<T> Drop for RingMmap<T> {
    fn drop(&mut self) {
        // SAFETY: ptr/len are exactly the mmap result owned by self.
        unsafe { libc::munmap(self.ptr.as_ptr().cast(), self.len) };
    }
}

/// Maps one AF_XDP ring.
///
/// # Safety
///
/// `fd`, `offset`, and `ring_offset` must describe a ring configured on the
/// same AF_XDP socket, and the resulting mapping must be accessed according to
/// AF_XDP's SPSC ownership rules.
pub unsafe fn mmap_ring<T>(
    fd: RawFd,
    desc_len: usize,
    offsets: &libc::xdp_ring_offset,
    ring_offset: u64,
) -> io::Result<RingMmap<T>> {
    let u32_len = mem::size_of::<AtomicU32>();
    let len = [
        offsets.producer as usize + u32_len,
        offsets.consumer as usize + u32_len,
        offsets.flags as usize + u32_len,
        offsets.desc as usize + desc_len,
    ]
    .into_iter()
    .max()
    .expect("ring mapping has at least one component");
    // SAFETY: caller guarantees fd/ring offset name a valid AF_XDP ring.
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_POPULATE,
            fd,
            ring_offset as libc::off_t,
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err(io::Error::last_os_error());
    }
    let ptr = NonNull::new(ptr.cast::<u8>()).expect("mmap never returns null on success");
    // SAFETY: offsets are supplied by the kernel for this mapping and point
    // inside the mapped ring layout.
    let producer = unsafe { ptr.as_ptr().add(offsets.producer as usize).cast() };
    // SAFETY: same kernel-provided ring layout as above.
    let consumer = unsafe { ptr.as_ptr().add(offsets.consumer as usize).cast() };
    // SAFETY: same kernel-provided ring layout as above.
    let desc = unsafe { ptr.as_ptr().add(offsets.desc as usize).cast() };
    // SAFETY: same kernel-provided ring layout as above.
    let flags = unsafe { ptr.as_ptr().add(offsets.flags as usize).cast() };

    Ok(RingMmap {
        producer,
        consumer,
        desc,
        flags,
        ptr,
        len,
        _marker: PhantomData,
    })
}

/// Userspace producer cursor for FILL and TX rings.
#[derive(Clone, Copy, Debug)]
pub struct RingProducer {
    producer: *mut AtomicU32,
    consumer: *mut AtomicU32,
    cached_producer: u32,
    cached_consumer: u32,
    size: u32,
}

impl RingProducer {
    /// Creates a producer cursor.
    #[must_use]
    pub fn new(producer: *mut AtomicU32, consumer: *mut AtomicU32, size: u32) -> Self {
        Self {
            producer,
            consumer,
            cached_producer: 0,
            cached_consumer: 0,
            size,
        }
    }

    /// Refreshes cached indexes.
    pub fn sync(&mut self, load_producer: bool) {
        // SAFETY: ring pointers come from a live AF_XDP mmap.
        unsafe {
            if load_producer {
                self.cached_producer = (*self.producer).load(Ordering::Acquire);
            }
            self.cached_consumer = (*self.consumer).load(Ordering::Acquire);
        }
    }

    /// Reserves one descriptor index.
    pub fn produce(&mut self) -> Option<u32> {
        if self.cached_producer.wrapping_sub(self.cached_consumer) >= self.size {
            self.sync(false);
            if self.cached_producer.wrapping_sub(self.cached_consumer) >= self.size {
                return None;
            }
        }
        let index = self.cached_producer;
        self.cached_producer = self.cached_producer.wrapping_add(1);
        Some(index)
    }

    /// Reserves up to `wanted` descriptor indexes.
    pub fn produce_many(&mut self, wanted: u32) -> RingRange {
        if wanted == 0 {
            return RingRange::default();
        }

        let mut available = self.available();
        if available < wanted {
            self.sync(false);
            available = self.available();
        }

        let count = wanted.min(available);
        let start = self.cached_producer;
        self.cached_producer = self.cached_producer.wrapping_add(count);
        RingRange { start, count }
    }

    /// Publishes produced descriptors.
    pub fn commit(&mut self) {
        // SAFETY: ring pointer comes from a live AF_XDP mmap.
        unsafe { (*self.producer).store(self.cached_producer, Ordering::Release) };
    }

    /// Returns free descriptor slots based on the cached indexes.
    #[must_use]
    pub fn available(&self) -> u32 {
        self.size
            .saturating_sub(self.cached_producer.wrapping_sub(self.cached_consumer))
    }
}

/// Userspace consumer cursor for RX and COMPLETION rings.
#[derive(Clone, Copy, Debug)]
pub struct RingConsumer {
    producer: *mut AtomicU32,
    consumer: *mut AtomicU32,
    cached_producer: u32,
    cached_consumer: u32,
}

impl RingConsumer {
    /// Creates a consumer cursor.
    #[must_use]
    pub fn new(producer: *mut AtomicU32, consumer: *mut AtomicU32) -> Self {
        Self {
            producer,
            consumer,
            cached_producer: 0,
            cached_consumer: 0,
        }
    }

    /// Refreshes producer index.
    #[inline]
    pub fn sync(&mut self) {
        // SAFETY: ring pointer comes from a live AF_XDP mmap.
        unsafe { self.cached_producer = (*self.producer).load(Ordering::Acquire) };
    }

    /// Reserves one descriptor index for consumption.
    pub fn consume(&mut self) -> Option<u32> {
        if self.cached_consumer == self.cached_producer {
            self.sync();
            if self.cached_consumer == self.cached_producer {
                return None;
            }
        }
        let index = self.cached_consumer;
        self.cached_consumer = self.cached_consumer.wrapping_add(1);
        Some(index)
    }

    /// Reserves up to `wanted` descriptor indexes for consumption.
    pub fn consume_many(&mut self, wanted: u32) -> RingRange {
        if wanted == 0 {
            return RingRange::default();
        }

        let mut available = self.available();
        if available < wanted {
            self.sync();
            available = self.available();
        }

        let count = wanted.min(available);
        let start = self.cached_consumer;
        self.cached_consumer = self.cached_consumer.wrapping_add(count);
        RingRange { start, count }
    }

    /// Publishes consumed descriptors.
    #[inline]
    pub fn release(&mut self) {
        // SAFETY: ring pointer comes from a live AF_XDP mmap.
        unsafe { (*self.consumer).store(self.cached_consumer, Ordering::Release) };
    }

    /// Returns available descriptors based on cached indexes.
    #[must_use]
    pub fn available(&self) -> u32 {
        self.cached_producer.wrapping_sub(self.cached_consumer)
    }
}

const _: () = {
    assert!(mem::size_of::<XdpDesc>() == 16);
};

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[test]
    fn producer_reserves_until_ring_is_full_and_commits() {
        let producer = AtomicU32::new(0);
        let consumer = AtomicU32::new(0);
        let mut cursor = RingProducer::new(
            (&producer as *const AtomicU32).cast_mut(),
            (&consumer as *const AtomicU32).cast_mut(),
            2,
        );

        assert_eq!(cursor.produce(), Some(0));
        assert_eq!(cursor.produce(), Some(1));
        assert_eq!(cursor.produce(), None);
        cursor.commit();
        assert_eq!(producer.load(Ordering::Acquire), 2);

        consumer.store(1, Ordering::Release);
        assert_eq!(cursor.produce(), Some(2));
    }

    #[test]
    fn producer_reserves_bulk_prefix_and_commits() {
        let producer = AtomicU32::new(0);
        let consumer = AtomicU32::new(0);
        let mut cursor = RingProducer::new(
            (&producer as *const AtomicU32).cast_mut(),
            (&consumer as *const AtomicU32).cast_mut(),
            4,
        );

        assert_eq!(cursor.produce_many(3), RingRange { start: 0, count: 3 });
        assert_eq!(cursor.produce_many(3), RingRange { start: 3, count: 1 });
        assert!(cursor.produce_many(1).is_empty());
        cursor.commit();
        assert_eq!(producer.load(Ordering::Acquire), 4);
    }

    #[test]
    fn consumer_reserves_available_entries_and_releases() {
        let producer = AtomicU32::new(2);
        let consumer = AtomicU32::new(0);
        let mut cursor = RingConsumer::new(
            (&producer as *const AtomicU32).cast_mut(),
            (&consumer as *const AtomicU32).cast_mut(),
        );

        assert_eq!(cursor.consume(), Some(0));
        assert_eq!(cursor.consume(), Some(1));
        assert_eq!(cursor.consume(), None);
        cursor.release();
        assert_eq!(consumer.load(Ordering::Acquire), 2);
    }

    #[test]
    fn consumer_reserves_bulk_prefix_and_releases() {
        let producer = AtomicU32::new(3);
        let consumer = AtomicU32::new(0);
        let mut cursor = RingConsumer::new(
            (&producer as *const AtomicU32).cast_mut(),
            (&consumer as *const AtomicU32).cast_mut(),
        );

        assert_eq!(cursor.consume_many(2), RingRange { start: 0, count: 2 });
        assert_eq!(cursor.consume_many(2), RingRange { start: 2, count: 1 });
        assert!(cursor.consume_many(1).is_empty());
        cursor.release();
        assert_eq!(consumer.load(Ordering::Acquire), 3);
    }
}
