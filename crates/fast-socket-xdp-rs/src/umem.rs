//! UMEM allocation and frame addressing.

use std::ffi::c_void;
use std::io;
use std::ops::{Deref, DerefMut};
use std::ptr;
use std::slice;

use fast_socket_rs::{HugePageSize, NumaNode};

const MPOL_F_STATIC_NODES: libc::c_int = 1 << 15;

/// UMEM allocation failure.
#[derive(Debug)]
pub struct AllocError;

impl std::fmt::Display for AllocError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("mmap failed while allocating UMEM")
    }
}

impl std::error::Error for AllocError {}

/// Page-aligned anonymous memory returned by `mmap`.
#[derive(Debug)]
pub struct PageAlignedMemory {
    ptr: *mut u8,
    len: usize,
}

// SAFETY: ownership of the mapping moves as a whole; interior synchronization is
// not provided and users must keep queue access single-threaded.
unsafe impl Send for PageAlignedMemory {}

impl PageAlignedMemory {
    /// Allocates `frame_size * frame_count` bytes, rounded up to `page_size`.
    pub fn alloc_with_page_size(
        frame_size: usize,
        frame_count: usize,
        page_size: usize,
        huge: bool,
    ) -> Result<Self, AllocError> {
        if !frame_size.is_power_of_two()
            || !frame_count.is_power_of_two()
            || !page_size.is_power_of_two()
        {
            return Err(AllocError);
        }

        let (ptr, aligned_size) = mmap_anonymous(frame_size, frame_count, page_size, huge)?;

        // SAFETY: `ptr` is valid for `aligned_size` bytes after successful
        // mmap, and zeroing does not read uninitialized memory.
        unsafe { ptr::write_bytes(ptr.cast::<u8>(), 0, aligned_size) };

        Ok(Self {
            ptr: ptr.cast::<u8>(),
            len: aligned_size,
        })
    }

    /// Allocates memory on a specific NUMA node.
    ///
    /// The mapping policy is bound before first touch and page placement is
    /// verified after faulting the pages in.
    pub fn alloc_with_page_size_on_numa_node(
        frame_size: usize,
        frame_count: usize,
        page_size: usize,
        huge: bool,
        numa_node: NumaNode,
    ) -> io::Result<Self> {
        if !frame_size.is_power_of_two()
            || !frame_count.is_power_of_two()
            || !page_size.is_power_of_two()
        {
            return Err(io::Error::other(AllocError.to_string()));
        }

        let (mapping, aligned_size) = mmap_anonymous(frame_size, frame_count, page_size, huge)
            .map_err(|_| io::Error::other(AllocError.to_string()))?;
        let mapping = mapping.cast::<u8>();
        let placement = (|| {
            // SAFETY: mapping/aligned_size identify the live anonymous mapping
            // returned by mmap and no page has been touched yet.
            unsafe { bind_mapping_to_numa_node(mapping, aligned_size, numa_node) }?;
            // SAFETY: mapping is valid for aligned_size bytes and the NUMA
            // policy has been applied before first touch.
            unsafe { ptr::write_bytes(mapping, 0, aligned_size) };
            verify_mapping_on_numa_node(mapping, aligned_size, page_size, numa_node)
        })();
        if let Err(error) = placement {
            // SAFETY: mapping/aligned_size are exactly the mmap result owned here.
            unsafe { libc::munmap(mapping.cast::<c_void>(), aligned_size) };
            return Err(error);
        }

        Ok(Self {
            ptr: mapping,
            len: aligned_size,
        })
    }

    /// Allocates using the operating-system page size.
    pub fn alloc(frame_size: usize, frame_count: usize) -> Result<Self, AllocError> {
        // SAFETY: sysconf is thread-safe.
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page_size <= 0 {
            return Err(AllocError);
        }
        Self::alloc_with_page_size(frame_size, frame_count, page_size as usize, false)
    }

    /// Allocates using the operating-system page size on a specific NUMA node.
    pub fn alloc_on_numa_node(
        frame_size: usize,
        frame_count: usize,
        numa_node: NumaNode,
    ) -> io::Result<Self> {
        // SAFETY: sysconf is thread-safe.
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page_size <= 0 {
            return Err(io::Error::other(AllocError.to_string()));
        }
        Self::alloc_with_page_size_on_numa_node(
            frame_size,
            frame_count,
            page_size as usize,
            false,
            numa_node,
        )
    }

    /// Returns the mapping length.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Returns true if the mapping is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Drop for PageAlignedMemory {
    fn drop(&mut self) {
        // SAFETY: `ptr` and `len` are exactly the mmap result owned by self.
        unsafe { libc::munmap(self.ptr.cast::<c_void>(), self.len) };
    }
}

impl Deref for PageAlignedMemory {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        // SAFETY: `ptr` is valid for `len` bytes while self lives.
        unsafe { slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl DerefMut for PageAlignedMemory {
    fn deref_mut(&mut self) -> &mut [u8] {
        // SAFETY: `ptr` is uniquely borrowed through &mut self.
        unsafe { slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

fn mmap_anonymous(
    frame_size: usize,
    frame_count: usize,
    page_size: usize,
    huge: bool,
) -> Result<(*mut c_void, usize), AllocError> {
    let memory_size = frame_size.checked_mul(frame_count).ok_or(AllocError)?;
    let aligned_size = memory_size.checked_add(page_size - 1).ok_or(AllocError)? & !(page_size - 1);
    let flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | if huge { libc::MAP_HUGETLB } else { 0 };

    // SAFETY: anonymous mmap with a null preferred address. The returned
    // mapping is checked against MAP_FAILED and owned by the caller on success.
    let ptr = unsafe {
        libc::mmap(
            ptr::null_mut(),
            aligned_size,
            libc::PROT_READ | libc::PROT_WRITE,
            flags,
            -1,
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err(AllocError);
    }
    Ok((ptr, aligned_size))
}

unsafe fn bind_mapping_to_numa_node(
    mapping: *mut u8,
    len: usize,
    node: NumaNode,
) -> io::Result<()> {
    let word_bits = usize::BITS as usize;
    let node = node.get() as usize;
    let word_index = node / word_bits;
    let bit = node % word_bits;
    let mut mask = vec![0 as libc::c_ulong; word_index + 1];
    mask[word_index] = 1u64.wrapping_shl(bit as u32) as libc::c_ulong;
    let mode = libc::MPOL_BIND | MPOL_F_STATIC_NODES;

    // SAFETY: the caller guarantees ptr/len identify a live mapping. Linux
    // decrements maxnode internally before copying the user nodemask, so pass
    // one extra bit to keep node 0 from being treated as an empty mask.
    let maxnode = node + 2;
    let rc = unsafe {
        libc::syscall(
            libc::SYS_mbind,
            mapping.cast::<c_void>(),
            len,
            mode,
            mask.as_ptr(),
            maxnode,
            0,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn verify_mapping_on_numa_node(
    mapping: *mut u8,
    len: usize,
    page_size: usize,
    node: NumaNode,
) -> io::Result<()> {
    let page_count = len / page_size;
    let mut pages = Vec::with_capacity(page_count);
    for index in 0..page_count {
        // SAFETY: each address is within the live mapping.
        pages.push(unsafe { mapping.add(index * page_size).cast::<c_void>() });
    }
    let mut status = vec![0 as libc::c_int; page_count];

    // SAFETY: pages/status are valid arrays with page_count entries. A null
    // nodes pointer requests placement status without moving pages.
    let rc = unsafe {
        libc::syscall(
            libc::SYS_move_pages,
            0,
            page_count,
            pages.as_ptr(),
            ptr::null::<libc::c_int>(),
            status.as_mut_ptr(),
            0,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }

    let expected = i32::from(node.get());
    if let Some((index, actual)) = status
        .iter()
        .copied()
        .enumerate()
        .find(|(_, actual)| *actual != expected)
    {
        let detail = if actual < 0 {
            format!("status error {}", io::Error::from_raw_os_error(-actual))
        } else {
            format!("node {actual}")
        };
        return Err(io::Error::other(format!(
            "UMEM page {index} is on {detail}, expected NUMA node {expected}",
        )));
    }

    Ok(())
}

/// UMEM region carved into fixed-size frames.
#[derive(Debug)]
pub struct Umem {
    backing: PageAlignedMemory,
    frame_size: u32,
    frame_count: u32,
}

impl Umem {
    /// Allocates a UMEM region.
    pub fn new(
        frame_size: u32,
        frame_count: u32,
        huge_page_size: HugePageSize,
    ) -> io::Result<Self> {
        if !frame_size.is_power_of_two() || !frame_count.is_power_of_two() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "UMEM frame size and frame count must be powers of two",
            ));
        }

        let huge_page = match huge_page_size {
            HugePageSize::Size4K => None,
            HugePageSize::Size1G => Some(1024 * 1024 * 1024),
            // Default and Size2M both prefer 2 MiB hugepages.
            _ => Some(2 * 1024 * 1024),
        };
        let backing = huge_page
            .and_then(|page| {
                PageAlignedMemory::alloc_with_page_size(
                    frame_size as usize,
                    frame_count as usize,
                    page,
                    true,
                )
                .ok()
            })
            .or_else(|| PageAlignedMemory::alloc(frame_size as usize, frame_count as usize).ok())
            .ok_or_else(|| io::Error::other(AllocError.to_string()))?;

        Ok(Self {
            backing,
            frame_size,
            frame_count,
        })
    }

    /// Allocates a UMEM region on a specific NUMA node.
    pub fn new_on_numa_node(
        frame_size: u32,
        frame_count: u32,
        huge_page_size: HugePageSize,
        numa_node: NumaNode,
    ) -> io::Result<Self> {
        if !frame_size.is_power_of_two() || !frame_count.is_power_of_two() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "UMEM frame size and frame count must be powers of two",
            ));
        }

        let huge_page = match huge_page_size {
            HugePageSize::Size4K => None,
            HugePageSize::Size1G => Some(1024 * 1024 * 1024),
            // Default and Size2M both prefer 2 MiB hugepages.
            _ => Some(2 * 1024 * 1024),
        };
        let mut huge_page_error = None;
        let backing = if let Some(page) = huge_page {
            match PageAlignedMemory::alloc_with_page_size_on_numa_node(
                frame_size as usize,
                frame_count as usize,
                page,
                true,
                numa_node,
            ) {
                Ok(backing) => Some(backing),
                Err(error) => {
                    huge_page_error = Some(error);
                    None
                }
            }
        } else {
            None
        };
        let backing = match backing {
            Some(backing) => backing,
            None => match PageAlignedMemory::alloc_on_numa_node(
                frame_size as usize,
                frame_count as usize,
                numa_node,
            ) {
                Ok(backing) => backing,
                Err(error) => {
                    if let Some(huge_page_error) = huge_page_error {
                        return Err(io::Error::other(format!(
                            "mmap failed while allocating UMEM on NUMA node {} \
                             (huge page attempt: {huge_page_error}; fallback attempt: {error})",
                            numa_node.get()
                        )));
                    }
                    return Err(error);
                }
            },
        };

        Ok(Self {
            backing,
            frame_size,
            frame_count,
        })
    }

    /// Returns the immutable base pointer.
    #[must_use]
    pub fn as_ptr(&self) -> *const u8 {
        self.backing.as_ptr()
    }

    /// Returns the mutable base pointer.
    #[must_use]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.backing.as_mut_ptr()
    }

    /// Returns the total mapping length.
    #[must_use]
    pub fn len(&self) -> usize {
        self.backing.len()
    }

    /// Returns true if the UMEM contains no bytes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.backing.is_empty()
    }

    /// Returns the frame size.
    #[must_use]
    pub const fn frame_size(&self) -> u32 {
        self.frame_size
    }

    /// Returns the frame count.
    #[must_use]
    pub const fn frame_count(&self) -> u32 {
        self.frame_count
    }

    /// Returns a frame byte offset suitable for XDP descriptors.
    #[must_use]
    pub fn frame_offset(&self, index: u32) -> u64 {
        debug_assert!(index < self.frame_count);
        u64::from(index) * u64::from(self.frame_size)
    }

    pub(crate) fn frame_addr_for_desc(&self, addr: u64) -> Option<u64> {
        let frame_size = u64::from(self.frame_size);
        let frame_addr = addr - (addr % frame_size);
        let frame_index = frame_addr / frame_size;
        (frame_index < u64::from(self.frame_count)).then_some(frame_addr)
    }

    pub(crate) fn descriptor_slice(&self, addr: u64, len: usize) -> Option<(u64, &[u8])> {
        let frame_addr = self.frame_addr_for_desc(addr)?;
        let start = usize::try_from(addr).ok()?;
        let end = start.checked_add(len)?;
        let frame_start = usize::try_from(frame_addr).ok()?;
        let frame_end = frame_start.checked_add(self.frame_size as usize)?;
        if start < frame_start || end > frame_end {
            return None;
        }
        self.backing
            .get(start..end)
            .map(|slice| (frame_addr, slice))
    }

    pub(crate) fn contains_frame_addr(&self, frame_addr: u64) -> bool {
        self.frame_addr_for_desc(frame_addr) == Some(frame_addr)
    }

    /// Returns bytes for a frame.
    #[must_use]
    pub fn frame(&self, index: u32) -> &[u8] {
        let start = index as usize * self.frame_size as usize;
        &self.backing[start..start + self.frame_size as usize]
    }

    /// Returns mutable bytes for a frame.
    #[must_use]
    pub fn frame_mut(&mut self, index: u32) -> &mut [u8] {
        let start = index as usize * self.frame_size as usize;
        &mut self.backing[start..start + self.frame_size as usize]
    }

    /// Returns bytes at a descriptor address.
    #[must_use]
    pub fn slice_at(&self, addr: u64, len: usize) -> &[u8] {
        let start = usize::try_from(addr).expect("UMEM descriptor address fits usize");
        let end = start
            .checked_add(len)
            .expect("UMEM descriptor range does not overflow usize");
        &self.backing[start..end]
    }

    /// Returns mutable bytes at a descriptor address.
    #[must_use]
    pub fn slice_at_mut(&mut self, addr: u64, len: usize) -> &mut [u8] {
        let start = usize::try_from(addr).expect("UMEM descriptor address fits usize");
        let end = start
            .checked_add(len)
            .expect("UMEM descriptor range does not overflow usize");
        &mut self.backing[start..end]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn umem_frame_offsets_and_slices_work() {
        let mut umem = Umem::new(2048, 4, HugePageSize::Default).unwrap();

        assert_eq!(umem.frame_size(), 2048);
        assert_eq!(umem.frame_count(), 4);
        assert_eq!(umem.frame_offset(0), 0);
        assert_eq!(umem.frame_offset(2), 4096);

        umem.frame_mut(1)[0..4].copy_from_slice(&[1, 2, 3, 4]);
        assert_eq!(umem.slice_at(umem.frame_offset(1), 4), &[1, 2, 3, 4]);
    }

    #[test]
    fn descriptor_slice_rejects_invalid_ranges() {
        let umem = Umem::new(2048, 4, HugePageSize::Default).unwrap();

        assert!(
            umem.descriptor_slice(umem.frame_offset(1) + 128, 64)
                .is_some()
        );
        assert!(
            umem.descriptor_slice(umem.frame_offset(1) + 2040, 16)
                .is_none()
        );
        assert!(
            umem.descriptor_slice(u64::from(umem.frame_size()) * 4, 1)
                .is_none()
        );
    }

    #[test]
    fn allocation_rejects_overflowing_alignment() {
        assert!(
            PageAlignedMemory::alloc_with_page_size(usize::MAX / 2 + 1, 2, 4096, false).is_err()
        );
        assert!(PageAlignedMemory::alloc_with_page_size(2048, 2, 0, false).is_err());
    }
}
