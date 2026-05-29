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
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum XdpMode {
    /// Let the kernel pick between zero-copy and copy modes based on driver
    /// support. Equivalent to passing neither `XDP_ZEROCOPY` nor `XDP_COPY`.
    #[default]
    Auto,
    /// Request driver zero-copy mode.
    ZeroCopy,
    /// Request copy mode.
    Copy,
}

impl XdpMode {
    fn flag(self) -> u16 {
        match self {
            Self::Auto => 0,
            Self::ZeroCopy => libc::XDP_ZEROCOPY,
            Self::Copy => libc::XDP_COPY,
        }
    }
}

/// How a [`RawXdpSocket`] obtains its UMEM at bind time.
enum UmemBinding<'a> {
    /// This socket registers the UMEM via `XDP_UMEM_REG` and binds as its owner.
    Owner {
        umem: &'a Umem,
        umem_headroom: u32,
    },
    /// This socket shares an already-registered UMEM owned by `shared_fd`,
    /// skipping `XDP_UMEM_REG` and binding with `XDP_SHARED_UMEM`.
    Shared { shared_fd: RawFd },
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
        Self::new_inner(
            if_index,
            queue_id,
            UmemBinding::Owner {
                umem,
                umem_headroom,
            },
            sizes,
            mode,
            pre_fill_frames,
        )
    }

    /// Opens, configures, mmaps, prefills, and binds an AF_XDP socket that
    /// **shares** an already-registered UMEM owned by another socket.
    ///
    /// This is the `members[1..]` path for an aggregate socket: it skips
    /// `XDP_UMEM_REG` (the owner socket at `shared_fd` already registered the
    /// mapping), sets up this socket's own FILL/COMPLETION/RX/TX rings, and
    /// binds with **only** `XDP_SHARED_UMEM`, pointing `sxdp_shared_umem_fd` at
    /// `shared_fd`. The shared member inherits the owner's copy/zero-copy mode
    /// and need-wakeup setting from the kernel; setting any mode or
    /// `XDP_USE_NEED_WAKEUP` flag on a shared bind is rejected with EINVAL. The
    /// caller owns the shared UMEM
    /// mapping and the frame pool that feeds `pre_fill_frames`; this socket
    /// holds no reference to either, so `shared_fd` must outlive the bind call
    /// only (the kernel pins the UMEM internally once bound).
    ///
    /// `shared_fd` must be the fd of a socket bound to the *same* UMEM, and the
    /// caller must ensure the frame addresses passed across all members of the
    /// shared UMEM are disjoint while in flight.
    pub fn new_shared_umem(
        if_index: u32,
        queue_id: u32,
        shared_fd: RawFd,
        sizes: RingSizes,
        mode: XdpMode,
        pre_fill_frames: impl IntoIterator<Item = u64>,
    ) -> io::Result<Self> {
        Self::new_inner(
            if_index,
            queue_id,
            UmemBinding::Shared { shared_fd },
            sizes,
            mode,
            pre_fill_frames,
        )
    }

    /// Shared open/configure/mmap/bind body for both the UMEM-owner and
    /// shared-UMEM paths. The only differences are whether `XDP_UMEM_REG` runs
    /// and the bind-time `XDP_SHARED_UMEM` flag / `sxdp_shared_umem_fd`.
    fn new_inner(
        if_index: u32,
        queue_id: u32,
        binding: UmemBinding<'_>,
        sizes: RingSizes,
        mode: XdpMode,
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

        // Owner registers the UMEM and binds with the requested copy/zero-copy
        // mode plus XDP_USE_NEED_WAKEUP. Shared members skip XDP_UMEM_REG and
        // bind with *only* XDP_SHARED_UMEM pointing at the owner fd: they MUST
        // NOT set XDP_COPY/XDP_ZEROCOPY *or* XDP_USE_NEED_WAKEUP. The kernel
        // rejects any of those on a shared bind with EINVAL (verified on a
        // 6.x/7.0 ice host) — a shared member inherits the owner pool's mode
        // and need-wakeup setting automatically in `xp_assign_dev_shared`.
        // (Note: this differs from the original design note, which incorrectly
        // listed XDP_USE_NEED_WAKEUP on the member bind.)
        let (shared_umem_fd, bind_flags) = match binding {
            UmemBinding::Owner {
                umem,
                umem_headroom,
            } => {
                let reg = libc::xdp_umem_reg {
                    addr: umem.as_ptr() as u64,
                    len: umem.len() as u64,
                    chunk_size: umem.frame_size(),
                    headroom: umem_headroom,
                    flags: 0,
                    tx_metadata_len: 0,
                };
                setsockopt(fd.as_raw_fd(), libc::SOL_XDP, libc::XDP_UMEM_REG, &reg)?;
                (0u32, libc::XDP_USE_NEED_WAKEUP | mode.flag())
            }
            UmemBinding::Shared { shared_fd } => (shared_fd as u32, libc::XDP_SHARED_UMEM),
        };

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
                fill_prod.commit();
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "pre_fill_frames exceeds FILL ring capacity {}",
                        sizes.fill
                    ),
                ));
            };
            // SAFETY: index was reserved from the FILL ring producer.
            unsafe { fill_mmap.desc.add((index & mask) as usize).write(addr) };
        }
        fill_prod.commit();

        let sxdp = libc::sockaddr_xdp {
            sxdp_family: libc::AF_XDP as libc::sa_family_t,
            sxdp_flags: bind_flags,
            sxdp_ifindex: if_index,
            sxdp_queue_id: queue_id,
            sxdp_shared_umem_fd: shared_umem_fd,
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

    /// Enables preferred busy polling on this socket fd.
    ///
    /// Sets `SO_PREFER_BUSY_POLL`, `SO_BUSY_POLL` (busy-poll time in
    /// microseconds), and `SO_BUSY_POLL_BUDGET` (max packets drained per
    /// busy-poll). This is the per-socket half of the AF_XDP busy-poll recipe;
    /// the socket already binds with `XDP_USE_NEED_WAKEUP`, which preferred
    /// busy polling requires. Per-netdev NAPI hard-IRQ deferral
    /// (`napi_defer_hard_irqs` / `gro_flush_timeout`) is a separate
    /// device-level concern configured by the factory, not here.
    ///
    /// `SO_PREFER_BUSY_POLL` and `SO_BUSY_POLL_BUDGET` require `CAP_NET_ADMIN`;
    /// callers without it will see `EPERM`.
    pub fn configure_busy_poll(&self, busy_poll_us: u32, budget: u32) -> io::Result<()> {
        let enable: libc::c_int = 1;
        setsockopt(
            self.fd.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PREFER_BUSY_POLL,
            &enable,
        )?;
        let busy_poll_us = clamp_to_c_int("busy_poll_us", busy_poll_us)?;
        setsockopt(
            self.fd.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_BUSY_POLL,
            &busy_poll_us,
        )?;
        let budget = clamp_to_c_int("busy_poll budget", budget)?;
        setsockopt(
            self.fd.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_BUSY_POLL_BUDGET,
            &budget,
        )?;
        Ok(())
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
        // `sizes.fill` is a non-zero power of two (RingSizes::validate), so
        // `-1` cannot underflow and produces the canonical ring mask.
        let mask = self.sizes.fill - 1;
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

    /// Drains up to `max` completion entries and hands them to `f` as the
    /// two contiguous slices that back the reserved range (the second slice
    /// is empty unless the range wraps the ring buffer).
    ///
    /// The closure runs once per drain (not per entry), so the caller pays
    /// no per-frame closure-dispatch overhead. The consumer cursor is
    /// released after the closure returns, regardless of whether it
    /// produced an error.
    pub(crate) fn drain_completion_slices<F, E>(&mut self, max: usize, f: F) -> Result<usize, E>
    where
        F: FnOnce(&[u64], &[u64]) -> Result<(), E>,
    {
        self.comp_cons.sync();
        let wanted = max.min(u32::MAX as usize) as u32;
        let range = self.comp_cons.consume_many(wanted);
        let completed = range.count as usize;
        if completed == 0 {
            return Ok(0);
        }

        let mask = self.sizes.completion - 1;
        let cap = self.sizes.completion as usize;
        let start = (range.start & mask) as usize;
        let first_len = (cap - start).min(completed);
        // SAFETY: the range was reserved from the COMPLETION consumer cursor;
        // the slice covers indices already published by the kernel.
        let first = unsafe { std::slice::from_raw_parts(self.comp_mmap.desc.add(start), first_len) };
        let second_len = completed - first_len;
        let second = if second_len > 0 {
            // SAFETY: the wrap-around tail starts at index 0 of the ring.
            unsafe { std::slice::from_raw_parts(self.comp_mmap.desc, second_len) }
        } else {
            &[]
        };

        let result = f(first, second);
        self.comp_cons.release();
        result?;
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
        let mask = self.sizes.tx - 1;
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

    /// Pokes the TX queue with a zero-length sendto doorbell.
    ///
    /// Earlier versions guarded on `tx_needs_wakeup()` first, but the kernel
    /// can set that flag *after* userspace publishes descriptors and *before*
    /// the next caller reads it. Skipping the doorbell in that race window
    /// leaves the just-committed TX entries parked indefinitely. The canonical
    /// xdpsock pattern is to always poke after publishing; transient errors
    /// (EAGAIN / EBUSY / ENETDOWN) are tolerated below since the descriptors
    /// are already in the ring and the next poke will retry.
    pub fn wake_tx(&self) -> io::Result<()> {
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

fn clamp_to_c_int(name: &str, value: u32) -> io::Result<libc::c_int> {
    libc::c_int::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("AF_XDP {name} value {value} exceeds c_int range"),
        )
    })
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

    #[test]
    fn new_shared_umem_validates_ring_sizes_before_touching_the_kernel() {
        // Invalid ring sizes must be rejected up front, before any AF_XDP
        // syscall runs, so this is checkable without a NIC. `shared_fd` is
        // never dereferenced on this path.
        let bad = RingSizes {
            fill: 3,
            ..RingSizes::default()
        };
        let err = RawXdpSocket::new_shared_umem(1, 0, -1, bad, XdpMode::Auto, std::iter::empty())
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }
}
