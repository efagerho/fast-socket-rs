//! Low-level AF_XDP socket setup.

use std::io;
use std::mem;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::ptr;

use crate::ring::{RingConsumer, RingMmap, RingProducer, RingRange, XdpDesc, mmap_ring};
use crate::umem::Umem;

/// AF_XDP ring capacities. All values must be powers of two.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RingSizes {
    /// FILL ring capacity.
    pub fill: u32,
    /// COMPLETION ring capacity.
    pub completion: u32,
    /// RX ring capacity.
    pub rx: u32,
    /// TX ring capacity.
    pub tx: u32,
}

impl Default for RingSizes {
    fn default() -> Self {
        Self {
            fill: 2048,
            completion: 2048,
            rx: 2048,
            tx: 2048,
        }
    }
}

impl RingSizes {
    /// Validates AF_XDP ring capacities for release-mode callers.
    pub fn validate(self) -> io::Result<()> {
        validate_ring_size("fill", self.fill)?;
        validate_ring_size("completion", self.completion)?;
        validate_ring_size("rx", self.rx)?;
        validate_ring_size("tx", self.tx)?;
        Ok(())
    }
}

/// AF_XDP bind mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum XdpMode {
    /// Request driver zero-copy mode.
    ZeroCopy,
    /// Request copy mode.
    Copy,
}

impl XdpMode {
    fn flag(self) -> u16 {
        match self {
            Self::ZeroCopy => libc::XDP_ZEROCOPY,
            Self::Copy => libc::XDP_COPY,
        }
    }
}

/// Kernel-facing AF_XDP socket and ring mappings.
#[derive(Debug)]
pub struct RawXdpSocket {
    fd: OwnedFd,
    if_index: u32,
    queue_id: u32,
    sizes: RingSizes,
    mode: XdpMode,
    /// FILL ring mapping.
    pub fill_mmap: RingMmap<u64>,
    /// COMPLETION ring mapping.
    pub comp_mmap: RingMmap<u64>,
    /// RX ring mapping.
    pub rx_mmap: RingMmap<XdpDesc>,
    /// TX ring mapping.
    pub tx_mmap: RingMmap<XdpDesc>,
    /// FILL producer cursor.
    pub fill_prod: RingProducer,
    /// COMPLETION consumer cursor.
    pub comp_cons: RingConsumer,
    /// RX consumer cursor.
    pub rx_cons: RingConsumer,
    /// TX producer cursor.
    pub tx_prod: RingProducer,
}

impl RawXdpSocket {
    /// Opens, configures, mmaps, prefills, and binds an AF_XDP socket.
    pub fn new(
        if_index: u32,
        queue_id: u32,
        umem: &mut Umem,
        sizes: RingSizes,
        mode: XdpMode,
        pre_fill_frames: impl IntoIterator<Item = u64>,
    ) -> io::Result<Self> {
        Self::new_with_umem_headroom(if_index, queue_id, umem, sizes, mode, 0, pre_fill_frames)
    }

    /// Opens, configures, mmaps, prefills, and binds an AF_XDP socket with UMEM headroom.
    pub fn new_with_umem_headroom(
        if_index: u32,
        queue_id: u32,
        umem: &mut Umem,
        sizes: RingSizes,
        mode: XdpMode,
        umem_headroom: u32,
        pre_fill_frames: impl IntoIterator<Item = u64>,
    ) -> io::Result<Self> {
        sizes.validate()?;

        // SAFETY: libc socket call returns an owned fd on success.
        let fd = unsafe { libc::socket(libc::AF_XDP, libc::SOCK_RAW, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: fd was just returned by socket and is uniquely owned.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };

        let reg = libc::xdp_umem_reg {
            addr: umem.as_ptr() as u64,
            len: umem.len() as u64,
            chunk_size: umem.frame_size(),
            headroom: umem_headroom,
            flags: 0,
            tx_metadata_len: 0,
        };
        setsockopt(fd.as_raw_fd(), libc::SOL_XDP, libc::XDP_UMEM_REG, &reg)?;

        for (opt, size) in [
            (libc::XDP_UMEM_COMPLETION_RING, sizes.completion),
            (libc::XDP_UMEM_FILL_RING, sizes.fill),
            (libc::XDP_RX_RING, sizes.rx),
            (libc::XDP_TX_RING, sizes.tx),
        ] {
            setsockopt(fd.as_raw_fd(), libc::SOL_XDP, opt, &size)?;
        }

        // SAFETY: zeroed is valid for xdp_mmap_offsets.
        let mut offsets: libc::xdp_mmap_offsets = unsafe { mem::zeroed() };
        let mut optlen = mem::size_of::<libc::xdp_mmap_offsets>() as libc::socklen_t;
        // SAFETY: offsets/optlen point to valid writable memory.
        let rc = unsafe {
            libc::getsockopt(
                fd.as_raw_fd(),
                libc::SOL_XDP,
                libc::XDP_MMAP_OFFSETS,
                (&mut offsets as *mut libc::xdp_mmap_offsets).cast(),
                &mut optlen,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }

        // SAFETY: fd has configured rings and offsets came from getsockopt.
        let fill_mmap = unsafe {
            mmap_ring::<u64>(
                fd.as_raw_fd(),
                sizes.fill as usize * mem::size_of::<u64>(),
                &offsets.fr,
                libc::XDP_UMEM_PGOFF_FILL_RING,
            )?
        };
        // SAFETY: same as above for completion ring.
        let comp_mmap = unsafe {
            mmap_ring::<u64>(
                fd.as_raw_fd(),
                sizes.completion as usize * mem::size_of::<u64>(),
                &offsets.cr,
                libc::XDP_UMEM_PGOFF_COMPLETION_RING,
            )?
        };
        // SAFETY: same as above for RX ring.
        let rx_mmap = unsafe {
            mmap_ring::<XdpDesc>(
                fd.as_raw_fd(),
                sizes.rx as usize * mem::size_of::<XdpDesc>(),
                &offsets.rx,
                libc::XDP_PGOFF_RX_RING as u64,
            )?
        };
        // SAFETY: same as above for TX ring.
        let tx_mmap = unsafe {
            mmap_ring::<XdpDesc>(
                fd.as_raw_fd(),
                sizes.tx as usize * mem::size_of::<XdpDesc>(),
                &offsets.tx,
                libc::XDP_PGOFF_TX_RING as u64,
            )?
        };

        let mut fill_prod = RingProducer::new(fill_mmap.producer, fill_mmap.consumer, sizes.fill);
        let comp_cons = RingConsumer::new(comp_mmap.producer, comp_mmap.consumer);
        let rx_cons = RingConsumer::new(rx_mmap.producer, rx_mmap.consumer);
        let tx_prod = RingProducer::new(tx_mmap.producer, tx_mmap.consumer, sizes.tx);

        let mask = sizes.fill - 1;
        for addr in pre_fill_frames {
            let Some(index) = fill_prod.produce() else {
                break;
            };
            // SAFETY: index was reserved from the FILL ring producer.
            unsafe { fill_mmap.desc.add((index & mask) as usize).write(addr) };
        }
        fill_prod.commit();

        let sxdp = libc::sockaddr_xdp {
            sxdp_family: libc::AF_XDP as libc::sa_family_t,
            sxdp_flags: libc::XDP_USE_NEED_WAKEUP | mode.flag(),
            sxdp_ifindex: if_index,
            sxdp_queue_id: queue_id,
            sxdp_shared_umem_fd: 0,
        };
        // SAFETY: sockaddr pointer is valid for the duration of bind.
        let rc = unsafe {
            libc::bind(
                fd.as_raw_fd(),
                (&sxdp as *const libc::sockaddr_xdp).cast(),
                mem::size_of::<libc::sockaddr_xdp>() as libc::socklen_t,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(Self {
            fd,
            if_index,
            queue_id,
            sizes,
            mode,
            fill_mmap,
            comp_mmap,
            rx_mmap,
            tx_mmap,
            fill_prod,
            comp_cons,
            rx_cons,
            tx_prod,
        })
    }

    /// Returns interface index.
    #[must_use]
    pub const fn if_index(&self) -> u32 {
        self.if_index
    }

    /// Returns queue id.
    #[must_use]
    pub const fn queue_id(&self) -> u32 {
        self.queue_id
    }

    /// Returns ring sizes.
    #[must_use]
    pub const fn sizes(&self) -> RingSizes {
        self.sizes
    }

    /// Returns selected XDP mode.
    #[must_use]
    pub const fn mode(&self) -> XdpMode {
        self.mode
    }

    /// Returns the raw AF_XDP fd.
    #[must_use]
    pub fn fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }

    /// Clones the AF_XDP fd for readiness-driver ownership.
    pub fn try_clone_fd(&self) -> io::Result<OwnedFd> {
        self.fd.try_clone()
    }

    /// Pushes frame addresses into the FILL ring.
    ///
    /// Returns the count published. The caller keeps any remaining addresses
    /// and retries after the kernel consumes more FILL entries.
    pub fn replenish_fill<I>(&mut self, addrs: I) -> u32
    where
        I: IntoIterator<Item = u64>,
    {
        self.fill_prod.sync(false);
        let mut written = 0;
        let mask = self.sizes.fill.saturating_sub(1);
        for addr in addrs {
            let Some(index) = self.fill_prod.produce() else {
                break;
            };
            // SAFETY: index was reserved from the FILL producer cursor.
            unsafe { self.fill_mmap.desc.add((index & mask) as usize).write(addr) };
            written += 1;
        }
        if written > 0 {
            self.fill_prod.commit();
        }
        written
    }

    /// Pushes a contiguous batch of frame addresses into the FILL ring.
    ///
    /// Returns the count published. The caller keeps any remaining addresses
    /// and retries after the kernel consumes more FILL entries.
    pub fn replenish_fill_batch(&mut self, addrs: &[u64]) -> usize {
        self.fill_prod.sync(false);
        let wanted = addrs.len().min(u32::MAX as usize) as u32;
        let range = self.fill_prod.produce_many(wanted);
        if range.is_empty() {
            return 0;
        }

        let written = range.count as usize;
        // SAFETY: the range was reserved from the FILL producer cursor, and
        // `written` is bounded by `addrs.len()`.
        unsafe {
            copy_slice_to_ring(
                self.fill_mmap.desc,
                self.sizes.fill,
                range.start,
                &addrs[..written],
            );
        }
        self.fill_prod.commit();
        written
    }

    /// Drains TX completion frame addresses into `out`.
    pub fn drain_completion(&mut self, out: &mut Vec<u64>) -> usize {
        self.comp_cons.sync();
        let wanted = out
            .capacity()
            .saturating_sub(out.len())
            .min(u32::MAX as usize) as u32;
        let range = self.comp_cons.consume_many(wanted);
        let completed = range.count as usize;
        if completed > 0 {
            // SAFETY: the range was reserved from the COMPLETION consumer
            // cursor, and `out` has at least `completed` spare capacity.
            unsafe {
                copy_ring_to_vec(self.comp_mmap.desc, self.sizes.completion, range, out);
            }
            self.comp_cons.release();
        }
        completed
    }

    pub(crate) fn drain_completion_for_each<E>(
        &mut self,
        max: usize,
        mut f: impl FnMut(u64) -> Result<(), E>,
    ) -> Result<usize, E> {
        self.comp_cons.sync();
        let wanted = max.min(u32::MAX as usize) as u32;
        let range = self.comp_cons.consume_many(wanted);
        let completed = range.count as usize;
        if completed == 0 {
            return Ok(0);
        }

        let mask = self.sizes.completion.saturating_sub(1);
        let mut error = None;
        for offset in 0..range.count {
            let index = range.start.wrapping_add(offset);
            // SAFETY: the range was reserved from the COMPLETION consumer cursor.
            let addr = unsafe { self.comp_mmap.desc.add((index & mask) as usize).read() };
            if error.is_none() {
                if let Err(err) = f(addr) {
                    error = Some(err);
                }
            }
        }
        self.comp_cons.release();

        if let Some(error) = error {
            return Err(error);
        }
        Ok(completed)
    }

    /// Drains up to `max` RX descriptors into `out`.
    pub fn drain_rx(&mut self, out: &mut Vec<XdpDesc>, max: usize) -> usize {
        self.rx_cons.sync();
        let wanted = max
            .min(out.capacity().saturating_sub(out.len()))
            .min(u32::MAX as usize) as u32;
        let range = self.rx_cons.consume_many(wanted);
        let drained = range.count as usize;
        if drained > 0 {
            // SAFETY: the range was reserved from the RX consumer cursor, and
            // `out` has at least `drained` spare capacity.
            unsafe {
                copy_ring_to_vec(self.rx_mmap.desc, self.sizes.rx, range, out);
            }
            self.rx_cons.release();
        }
        drained
    }

    /// Enqueues one TX descriptor.
    ///
    /// Returns `false` when the TX ring is full. Call [`Self::commit_tx`]
    /// after enqueueing a batch.
    pub fn enqueue_tx(&mut self, desc: XdpDesc) -> bool {
        let Some(index) = self.tx_prod.produce() else {
            return false;
        };
        let mask = self.sizes.tx.saturating_sub(1);
        // SAFETY: index was reserved from the TX producer cursor.
        unsafe { self.tx_mmap.desc.add((index & mask) as usize).write(desc) };
        true
    }

    /// Enqueues a contiguous batch of TX descriptors.
    ///
    /// Returns the number of descriptors staged. Call [`Self::commit_tx`] after
    /// enqueueing a batch.
    pub fn enqueue_tx_batch(&mut self, descs: &[XdpDesc]) -> usize {
        let wanted = descs.len().min(u32::MAX as usize) as u32;
        let range = self.tx_prod.produce_many(wanted);
        if range.is_empty() {
            return 0;
        }

        let staged = range.count as usize;
        // SAFETY: the range was reserved from the TX producer cursor, and
        // `staged` is bounded by `descs.len()`.
        unsafe {
            copy_slice_to_ring(
                self.tx_mmap.desc,
                self.sizes.tx,
                range.start,
                &descs[..staged],
            );
        }
        staged
    }

    /// Publishes TX descriptors previously enqueued with [`Self::enqueue_tx`].
    #[inline]
    pub fn commit_tx(&mut self) {
        self.tx_prod.commit();
    }

    /// Returns currently available TX descriptors after refreshing the cursor.
    pub fn tx_available(&mut self) -> u32 {
        self.tx_prod.sync(false);
        self.tx_prod.available()
    }

    /// Returns true when the kernel asks userspace to kick TX.
    #[must_use]
    pub fn tx_needs_wakeup(&self) -> bool {
        self.tx_mmap.needs_wakeup()
    }

    /// Returns true when the kernel asks userspace to kick RX/FILL.
    #[must_use]
    pub fn fill_needs_wakeup(&self) -> bool {
        self.fill_mmap.needs_wakeup()
    }

    /// Nudge RX after publishing FILL descriptors.
    pub fn wake_rx(&self) -> io::Result<()> {
        let mut pfd = libc::pollfd {
            fd: self.fd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: pfd points to a valid single-element pollfd array.
        let rc = unsafe { libc::poll(&mut pfd, 1, 0) };
        if rc < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }

    /// Wakes TX when NEED_WAKEUP is set.
    pub fn wake_tx(&self) -> io::Result<()> {
        if !self.tx_needs_wakeup() {
            return Ok(());
        }
        // SAFETY: sendto with null buffer/zero length is the documented AF_XDP
        // doorbell nudge and does not retain pointers.
        let rc = unsafe {
            libc::sendto(
                self.fd.as_raw_fd(),
                ptr::null(),
                0,
                libc::MSG_DONTWAIT,
                ptr::null(),
                0,
            )
        };
        if rc < 0 {
            let error = io::Error::last_os_error();
            match error.raw_os_error() {
                // The TX descriptors are already published. Some drivers can
                // report ENETDOWN from the zero-length AF_XDP doorbell while
                // the queue is otherwise usable under multi-queue load.
                Some(libc::EAGAIN) | Some(libc::EBUSY) | Some(libc::ENETDOWN) => {}
                _ => return Err(error),
            }
        }
        Ok(())
    }
}

impl AsFd for RawXdpSocket {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }
}

fn setsockopt<T>(fd: i32, level: i32, opt: i32, value: &T) -> io::Result<()> {
    // SAFETY: value points to a valid T for the duration of the call.
    let rc = unsafe {
        libc::setsockopt(
            fd,
            level,
            opt,
            (value as *const T).cast(),
            mem::size_of::<T>() as libc::socklen_t,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

unsafe fn copy_slice_to_ring<T: Copy>(ring: *mut T, size: u32, start: u32, src: &[T]) {
    debug_assert!(size.is_power_of_two());
    let mask = size - 1;
    let mut copied = 0;
    while copied < src.len() {
        let ring_index = (start.wrapping_add(copied as u32) & mask) as usize;
        let chunk = (src.len() - copied).min(size as usize - ring_index);
        // SAFETY: caller guarantees this absolute range was reserved from the
        // ring cursor. The chunk is clipped to the ring boundary.
        unsafe {
            ptr::copy_nonoverlapping(src.as_ptr().add(copied), ring.add(ring_index), chunk);
        }
        copied += chunk;
    }
}

unsafe fn copy_ring_to_vec<T: Copy>(ring: *const T, size: u32, range: RingRange, out: &mut Vec<T>) {
    debug_assert!(size.is_power_of_two());
    let count = range.count as usize;
    debug_assert!(out.capacity().saturating_sub(out.len()) >= count);

    let old_len = out.len();
    let mask = size - 1;
    let mut copied = 0;
    while copied < count {
        let ring_index = (range.start.wrapping_add(copied as u32) & mask) as usize;
        let chunk = (count - copied).min(size as usize - ring_index);
        // SAFETY: caller guarantees this absolute range was reserved from the
        // ring cursor and `out` has enough spare capacity.
        unsafe {
            ptr::copy_nonoverlapping(
                ring.add(ring_index),
                out.as_mut_ptr().add(old_len + copied),
                chunk,
            );
        }
        copied += chunk;
    }
    // SAFETY: the copied `T: Copy` entries initialized exactly `count` new
    // elements in the vector spare capacity.
    unsafe { out.set_len(old_len + count) };
}

fn validate_ring_size(name: &str, size: u32) -> io::Result<()> {
    if size.is_power_of_two() {
        return Ok(());
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("AF_XDP {name} ring size {size} is not a non-zero power of two"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_sizes_reject_zero_and_non_power_of_two_values() {
        assert!(RingSizes::default().validate().is_ok());

        let zero = RingSizes {
            rx: 0,
            ..RingSizes::default()
        };
        assert_eq!(
            zero.validate().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );

        let non_power_of_two = RingSizes {
            tx: 3,
            ..RingSizes::default()
        };
        assert_eq!(
            non_power_of_two.validate().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}
