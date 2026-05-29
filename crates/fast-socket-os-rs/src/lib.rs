//! OS-backed socket implementation crate.
//!
//! This crate provides the direct OS-backed `UdpSocket` implementation. It
//! depends on the backend-agnostic `fast-socket-rs` core crate and does not
//! define replacement core traits.

#![deny(missing_docs)]

mod buffer;

use std::io;
use std::marker::PhantomData;
use std::net::{
    IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6, UdpSocket as StdUdpSocket,
};
use std::rc::Rc;
use std::time::Duration;

use fast_socket_rs::{
    BufferLayout, BufferPool, DeviceError, DeviceErrorKind, Error, IfIndex, PacketBuffer,
    QueueAffinity, QueueId, ReadinessDriver, ReadinessSource, RecvBatch, SendError, SocketId,
    TxSlot, UdpCapabilities, UdpReceive, UdpRecvMeta, UdpSocket, UdpTransmit, WaitOutcome,
    WakeHandle,
};

pub use buffer::{OsBufferPool, OsPacketBuf, OsPacketBufMut};

#[cfg(unix)]
use std::os::fd::{AsFd, AsRawFd};
#[cfg(unix)]
use std::os::raw::{c_int, c_short};

/// Default per-socket batch size. Picked to amortize the typical
/// `recvmmsg` / `sendmmsg` syscall overhead (~1 µs) while keeping per-socket
/// memory under ~24 KiB. Configurable per socket via
/// [`OsUdpSocketConfig::max_batch`].
pub const DEFAULT_MAX_BATCH: usize = 64;

/// Upper bound on [`OsUdpSocketConfig::max_batch`]. Caps per-socket memory at
/// roughly 4096 × 384 B ≈ 1.5 MiB even for a misconfigured callsite.
pub const MAX_BATCH_HARD_CAP: usize = 4096;
/// Size of the per-message control buffer used to receive IP_PKTINFO /
/// IPV6_PKTINFO ancillary data on Linux. 128 bytes is generous: a single
/// `cmsghdr` + `in6_pktinfo` plus alignment fits in well under 64.
#[cfg(target_os = "linux")]
const RECV_CMSG_LEN: usize = 128;

#[cfg(all(
    unix,
    any(
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "ios",
        target_os = "macos",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "tvos",
        target_os = "watchos"
    )
))]
type Nfds = std::os::raw::c_uint;

#[cfg(all(
    unix,
    not(any(
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "ios",
        target_os = "macos",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "tvos",
        target_os = "watchos"
    ))
))]
type Nfds = usize;

/// Builder for a direct OS-backed UDP socket.
#[derive(Clone, Debug)]
pub struct OsUdpSocketBuilder {
    bind_addr: SocketAddr,
    if_index: Option<IfIndex>,
    queue_id: QueueId,
    queue_affinity: QueueAffinity,
    rx_buffer_layout: BufferLayout,
    tx_buffer_layout: BufferLayout,
    mtu: usize,
    reuse_port: bool,
    max_batch: usize,
}

impl OsUdpSocketBuilder {
    /// Creates a builder that binds a UDP socket to `bind_addr`.
    #[must_use]
    pub fn new(bind_addr: SocketAddr) -> Self {
        Self {
            bind_addr,
            if_index: None,
            queue_id: QueueId::new(0),
            queue_affinity: QueueAffinity::Any,
            rx_buffer_layout: BufferLayout::new(2048),
            tx_buffer_layout: BufferLayout::new(2048),
            mtu: 1472,
            reuse_port: false,
            max_batch: DEFAULT_MAX_BATCH,
        }
    }

    /// Records the operating-system interface selected for this socket.
    ///
    /// The portable builder does not perform platform-specific device binding.
    /// Callers that need that behavior can preconfigure a socket and pass it to
    /// [`OsUdpSocket::from_std`] with the same index in [`OsUdpSocketConfig`].
    #[must_use]
    pub const fn if_index(mut self, if_index: IfIndex) -> Self {
        self.if_index = Some(if_index);
        self
    }

    /// Sets the logical queue identifier for the constructed socket.
    #[must_use]
    pub const fn queue_id(mut self, queue_id: QueueId) -> Self {
        self.queue_id = queue_id;
        self
    }

    /// Sets the CPU affinity hint for the selected queue.
    ///
    /// On Linux, [`QueueAffinity::Core`] maps to `SO_INCOMING_CPU` so the
    /// kernel steers packets for this socket to the requested CPU when the
    /// platform supports it. Other affinity forms are retained as metadata and
    /// do not change OS socket options.
    #[must_use]
    pub const fn queue_affinity(mut self, queue_affinity: QueueAffinity) -> Self {
        self.queue_affinity = queue_affinity;
        self
    }

    /// Sets the heap buffer layout used by both copy-based pools.
    #[must_use]
    pub const fn buffer_layout(mut self, buffer_layout: BufferLayout) -> Self {
        self.rx_buffer_layout = buffer_layout;
        self.tx_buffer_layout = buffer_layout;
        self
    }

    /// Sets the heap buffer layout used by the receive path.
    #[must_use]
    pub const fn rx_buffer_layout(mut self, rx_buffer_layout: BufferLayout) -> Self {
        self.rx_buffer_layout = rx_buffer_layout;
        self
    }

    /// Sets the heap buffer layout used by the transmit path.
    #[must_use]
    pub const fn tx_buffer_layout(mut self, tx_buffer_layout: BufferLayout) -> Self {
        self.tx_buffer_layout = tx_buffer_layout;
        self
    }

    /// Sets the effective UDP payload MTU reported by the socket.
    #[must_use]
    pub const fn mtu(mut self, mtu: usize) -> Self {
        self.mtu = mtu;
        self
    }

    /// Enables or disables `SO_REUSEPORT` before binding the socket.
    ///
    /// When enabled on platforms that support it, multiple sockets created by
    /// compatible processes can bind the same UDP address and port. This is
    /// useful for per-core OS receive workers while keeping one destination
    /// port visible to senders.
    #[must_use]
    pub const fn reuse_port(mut self, reuse_port: bool) -> Self {
        self.reuse_port = reuse_port;
        self
    }

    /// Sets the per-syscall batch size used by `recvmmsg` / `sendmmsg` and
    /// the pre-allocated state arrays.
    ///
    /// Each socket holds roughly `384 × max_batch` bytes of resident state
    /// (sockaddr_storage + mmsghdr + iovec + cmsg buffer per slot). The
    /// default of [`DEFAULT_MAX_BATCH`] (64) keeps a socket under ~24 KiB
    /// and is a good balance for typical NIC-line-rate UDP workloads.
    /// Larger values amortize syscall cost across bigger bursts; smaller
    /// values reduce per-socket memory for many-connection workloads.
    ///
    /// Capped at [`MAX_BATCH_HARD_CAP`]; zero is rejected at bind time.
    #[must_use]
    pub const fn max_batch(mut self, max_batch: usize) -> Self {
        self.max_batch = max_batch;
        self
    }

    /// Binds and opens the live OS-backed UDP socket.
    pub fn bind(self) -> io::Result<OsUdpSocket> {
        let socket = bind_udp_socket(self.bind_addr, self.reuse_port)?;
        OsUdpSocket::from_std(
            socket,
            OsUdpSocketConfig {
                if_index: self.if_index,
                queue_id: self.queue_id,
                queue_affinity: self.queue_affinity,
                rx_buffer_layout: self.rx_buffer_layout,
                tx_buffer_layout: self.tx_buffer_layout,
                mtu: self.mtu,
                max_batch: self.max_batch,
            },
        )
    }
}

/// Configuration used when wrapping an existing [`std::net::UdpSocket`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OsUdpSocketConfig {
    /// Operating-system interface selected for this socket, when known.
    pub if_index: Option<IfIndex>,
    /// Logical queue identifier reported by the socket.
    pub queue_id: QueueId,
    /// CPU affinity hint for the socket's receive queue.
    pub queue_affinity: QueueAffinity,
    /// Heap buffer layout used for receive buffers.
    pub rx_buffer_layout: BufferLayout,
    /// Heap buffer layout used for transmit buffers.
    pub tx_buffer_layout: BufferLayout,
    /// Effective UDP payload MTU reported by the socket.
    pub mtu: usize,
    /// Per-syscall batch size for `recvmmsg` / `sendmmsg`. Bounded by
    /// [`MAX_BATCH_HARD_CAP`]; zero is rejected at bind time. See
    /// [`OsUdpSocketBuilder::max_batch`] for sizing guidance.
    pub max_batch: usize,
}

impl Default for OsUdpSocketConfig {
    fn default() -> Self {
        Self {
            if_index: None,
            queue_id: QueueId::new(0),
            queue_affinity: QueueAffinity::Any,
            rx_buffer_layout: BufferLayout::new(2048),
            tx_buffer_layout: BufferLayout::new(2048),
            mtu: 1472,
            max_batch: DEFAULT_MAX_BATCH,
        }
    }
}

/// Direct OS-backed UDP socket implementation.
///
/// Drop order: `socket` first, then the pools and their cloned `Rc` references.
/// Frozen and mutable packets handed out to callers each hold an `Rc` to the
/// pool's interior, so the pool stays alive until every outstanding buffer is
/// dropped even if the socket is dropped earlier.
#[derive(Debug)]
pub struct OsUdpSocket {
    socket: StdUdpSocket,
    rx_pool: OsBufferPool,
    tx_pool: OsBufferPool,
    driver: ReadinessDriver<OsReadinessSource>,
    if_index: Option<IfIndex>,
    queue_id: QueueId,
    queue_affinity: QueueAffinity,
    mtu: usize,
    /// Per-syscall cap; same value chunks both `recvmmsg` and `sendmmsg` and
    /// sizes every per-message scratch array below.
    max_batch: usize,
    recv_buffers: Box<[Option<OsPacketBufMut>]>,
    #[cfg(target_os = "linux")]
    raw_fd: std::os::fd::RawFd,
    #[cfg(target_os = "linux")]
    recv_addrs: Box<[libc::sockaddr_storage]>,
    #[cfg(target_os = "linux")]
    recv_iovs: Box<[libc::iovec]>,
    #[cfg(target_os = "linux")]
    recv_hdrs: Box<[libc::mmsghdr]>,
    /// Per-message control buffers; sized for one `IP_PKTINFO` or
    /// `IPV6_PKTINFO` ancillary message so the receive path can decode the
    /// arrival destination IP/ifindex. Accessed indirectly through the
    /// `msg_control` raw pointer stored in `recv_hdrs[i].msg_hdr`; this
    /// field owns the backing storage.
    #[cfg(target_os = "linux")]
    #[allow(dead_code)]
    recv_cmsgs: Box<[[u8; RECV_CMSG_LEN]]>,
    #[cfg(target_os = "linux")]
    tx_addrs: Box<[libc::sockaddr_storage]>,
    #[cfg(target_os = "linux")]
    tx_iovs: Vec<libc::iovec>,
    /// Per-message `(iov_start, iov_count)` slices into `tx_iovs` for the current
    /// `sendmmsg` chunk.
    #[cfg(target_os = "linux")]
    tx_iov_ranges: Box<[(usize, usize)]>,
    #[cfg(target_os = "linux")]
    tx_hdrs: Box<[libc::mmsghdr]>,
    _not_send: PhantomData<Rc<()>>,
}

impl OsUdpSocket {
    /// Wraps an existing standard UDP socket.
    ///
    /// The socket is put into nonblocking mode. The resulting live socket is
    /// intentionally `!Send`; construct it on the worker thread that will own it.
    pub fn from_std(socket: StdUdpSocket, config: OsUdpSocketConfig) -> io::Result<Self> {
        if config.max_batch == 0 || config.max_batch > MAX_BATCH_HARD_CAP {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "OsUdpSocketConfig::max_batch must be in 1..={MAX_BATCH_HARD_CAP}, got {}",
                    config.max_batch
                ),
            ));
        }
        configure_queue_affinity(&socket, config.queue_affinity)?;
        #[cfg(target_os = "linux")]
        enable_pktinfo(&socket)?;
        socket.set_nonblocking(true)?;
        let readiness_socket = socket.try_clone()?;
        #[cfg(target_os = "linux")]
        let raw_fd = socket.as_raw_fd();
        #[cfg(target_os = "linux")]
        let recv_state = build_recv_state(config.max_batch);
        #[cfg(target_os = "linux")]
        let send_state = build_send_state(config.max_batch);

        Ok(Self {
            socket,
            rx_pool: OsBufferPool::new(config.rx_buffer_layout),
            tx_pool: OsBufferPool::new(config.tx_buffer_layout),
            driver: ReadinessDriver::new(OsReadinessSource::new(readiness_socket)),
            if_index: config.if_index,
            queue_id: config.queue_id,
            queue_affinity: config.queue_affinity,
            mtu: config.mtu,
            max_batch: config.max_batch,
            recv_buffers: (0..config.max_batch).map(|_| None).collect::<Vec<_>>().into(),
            #[cfg(target_os = "linux")]
            raw_fd,
            #[cfg(target_os = "linux")]
            recv_addrs: recv_state.addrs,
            #[cfg(target_os = "linux")]
            recv_iovs: recv_state.iovs,
            #[cfg(target_os = "linux")]
            recv_hdrs: recv_state.hdrs,
            #[cfg(target_os = "linux")]
            recv_cmsgs: recv_state.cmsgs,
            #[cfg(target_os = "linux")]
            tx_addrs: send_state.addrs,
            #[cfg(target_os = "linux")]
            tx_iovs: Vec::with_capacity(config.max_batch),
            #[cfg(target_os = "linux")]
            tx_iov_ranges: send_state.iov_ranges,
            #[cfg(target_os = "linux")]
            tx_hdrs: send_state.hdrs,
            _not_send: PhantomData,
        })
    }

    /// Returns the local socket address.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Returns the configured operating-system interface index, when known.
    #[must_use]
    pub const fn if_index(&self) -> Option<IfIndex> {
        self.if_index
    }

    /// Returns the configured queue CPU affinity hint.
    #[must_use]
    pub const fn queue_affinity(&self) -> QueueAffinity {
        self.queue_affinity
    }

    /// Returns the wrapped standard UDP socket.
    #[must_use]
    pub const fn std_socket(&self) -> &StdUdpSocket {
        &self.socket
    }
}

impl UdpSocket for OsUdpSocket {
    type RxPool = OsBufferPool;
    type TxPool = OsBufferPool;
    type Driver = ReadinessDriver<OsReadinessSource>;
    type RecvMeta = UdpRecvMeta;

    fn socket_id(&self) -> SocketId {
        SocketId::new(self.queue_id.get())
    }

    fn worker_affinity(&self) -> QueueAffinity {
        self.queue_affinity
    }

    fn mtu(&self) -> usize {
        self.mtu
    }

    fn capabilities(&self) -> UdpCapabilities {
        UdpCapabilities::default()
    }

    fn rx_pool(&self) -> &Self::RxPool {
        &self.rx_pool
    }

    fn rx_pool_mut(&mut self) -> &mut Self::RxPool {
        &mut self.rx_pool
    }

    fn tx_pool(&self) -> &Self::TxPool {
        &self.tx_pool
    }

    fn tx_pool_mut(&mut self) -> &mut Self::TxPool {
        &mut self.tx_pool
    }

    fn driver(&self) -> &Self::Driver {
        &self.driver
    }

    fn driver_mut(&mut self) -> &mut Self::Driver {
        &mut self.driver
    }

    fn send(&mut self, batch: &mut [TxSlot<UdpTransmit<OsPacketBuf>>]) -> Result<usize, SendError> {
        self.send_impl(batch)
    }

    fn recv(
        &mut self,
        out: &mut RecvBatch<UdpReceive<OsPacketBufMut, Self::RecvMeta>>,
    ) -> Result<usize, Error> {
        self.recv_impl(out)
    }

    #[inline(always)]
    fn drain_tx_completions(&mut self) -> Result<usize, Error> {
        Ok(0)
    }
}

impl OsUdpSocket {
    #[cfg(target_os = "linux")]
    fn send_impl(
        &mut self,
        batch: &mut [TxSlot<UdpTransmit<OsPacketBuf>>],
    ) -> Result<usize, SendError> {
        let mut accepted = 0;

        while accepted < batch.len() {
            // Build the longest valid prefix of the remaining batch that we can
            // hand to one `sendmmsg`. A bad slot at the very start of a new
            // chunk surfaces as `SendError` immediately; a bad slot in the
            // middle ends the current chunk so the good prefix still flushes,
            // and the bad slot is re-evaluated as the head of the next chunk.
            let max_chunk_len = (batch.len() - accepted).min(self.max_batch);
            let mut chunk_len = 0;
            while chunk_len < max_chunk_len {
                let slot = &batch[accepted + chunk_len];
                let validation = match slot.as_ref() {
                    Some(tx) if tx.packet.len() > self.mtu => Err(Error::OversizeForMtu),
                    Some(tx) if has_unsupported_tx_options(tx) => Err(Error::InvalidPacket),
                    Some(_) => Ok(()),
                    None => Err(Error::InvalidBatch),
                };

                match validation {
                    Ok(()) => chunk_len += 1,
                    Err(kind) if chunk_len == 0 => return Err(SendError { accepted, kind }),
                    Err(_) => break,
                }
            }

            let chunk = &mut batch[accepted..accepted + chunk_len];
            let mut total_segments = 0usize;
            for slot in chunk.iter() {
                let tx = slot.as_ref().expect("validated ready slot");
                total_segments += tx.packet.segments().len();
            }

            self.tx_iovs.clear();
            self.tx_iovs.reserve(total_segments);
            for (index, slot) in chunk.iter().enumerate() {
                let tx = slot.as_ref().expect("validated ready slot");
                let start = self.tx_iovs.len();
                for segment in tx.packet.segments() {
                    self.tx_iovs.push(libc::iovec {
                        iov_base: segment.as_ptr().cast_mut().cast(),
                        iov_len: segment.len(),
                    });
                }
                self.tx_iov_ranges[index] = (start, self.tx_iovs.len() - start);
            }

            let iov_base = self.tx_iovs.as_mut_ptr();
            for (index, slot) in chunk.iter().enumerate() {
                let tx = slot.as_ref().expect("validated ready slot");
                let addr_len = sockaddr_from_socketaddr(&tx.destination, &mut self.tx_addrs[index]);
                let (iov_start, iov_count) = self.tx_iov_ranges[index];
                let msg = &mut self.tx_hdrs[index].msg_hdr;
                msg.msg_name = (&raw mut self.tx_addrs[index]).cast();
                msg.msg_namelen = addr_len;
                msg.msg_iov = unsafe { iov_base.add(iov_start) };
                msg.msg_iovlen = iov_count;
                msg.msg_control = std::ptr::null_mut();
                msg.msg_controllen = 0;
            }

            // Retry on EINTR before reporting back-pressure: EINTR with
            // `MSG_DONTWAIT` means no packets were dispatched, so a syscall
            // restart is safe and avoids spurious zero-progress returns.
            let sent = loop {
                // SAFETY: `tx_hdrs` and `tx_addrs`/`tx_iovs` are valid for
                // `chunk_len` entries (built immediately above) and the kernel
                // does not retain any of these pointers after sendmmsg returns.
                let result = unsafe {
                    libc::sendmmsg(
                        self.raw_fd,
                        self.tx_hdrs.as_mut_ptr(),
                        chunk_len as libc::c_uint,
                        libc::MSG_DONTWAIT,
                    )
                };
                if result < 0 {
                    let error = io::Error::last_os_error();
                    if error.kind() == io::ErrorKind::Interrupted {
                        continue;
                    }
                    if is_transient(&error) {
                        return Ok(accepted);
                    }
                    return Err(SendError {
                        accepted,
                        kind: io_error_to_core(error),
                    });
                }
                break result;
            };

            let sent = sent as usize;
            for slot in chunk.iter_mut().take(sent) {
                let _ = slot.take();
            }
            accepted += sent;
            if sent < chunk_len {
                // A short `sendmmsg` return means the (sent+1)-th packet was
                // rejected by the kernel; `sendmmsg` succeeds-then-stops and
                // does not surface the per-packet errno. Issue `sendmsg` on
                // just that slot to recover the real error, retrying on EINTR.
                let result = loop {
                    let r = unsafe {
                        libc::sendmsg(
                            self.raw_fd,
                            &self.tx_hdrs[sent].msg_hdr,
                            libc::MSG_DONTWAIT,
                        )
                    };
                    if r < 0 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
                        continue;
                    }
                    break r;
                };
                if result >= 0 {
                    let _ = chunk[sent].take();
                    accepted += 1;
                    continue;
                }
                let error = io::Error::last_os_error();
                if is_transient(&error) {
                    return Ok(accepted);
                }
                return Err(SendError {
                    accepted,
                    kind: io_error_to_core(error),
                });
            }
        }

        Ok(accepted)
    }

    #[cfg(not(target_os = "linux"))]
    fn send_impl(
        &mut self,
        batch: &mut [TxSlot<UdpTransmit<OsPacketBuf>>],
    ) -> Result<usize, SendError> {
        let mut accepted = 0;

        for slot in batch.iter_mut() {
            let Some(tx) = slot.as_ref() else {
                return Err(SendError {
                    accepted,
                    kind: Error::InvalidBatch,
                });
            };

            if tx.packet.len() > self.mtu {
                return Err(SendError {
                    accepted,
                    kind: Error::OversizeForMtu,
                });
            }

            if has_unsupported_tx_options(tx) {
                return Err(SendError {
                    accepted,
                    kind: Error::InvalidPacket,
                });
            }

            match self.socket.send_to(tx.packet.as_slice(), tx.destination) {
                Ok(_) => {
                    let _ = slot.take();
                    accepted += 1;
                }
                Err(error) if is_transient(&error) => return Ok(accepted),
                Err(error) => {
                    return Err(SendError {
                        accepted,
                        kind: io_error_to_core(error),
                    });
                }
            }
        }

        Ok(accepted)
    }

    #[cfg(target_os = "linux")]
    fn recv_impl(
        &mut self,
        out: &mut RecvBatch<UdpReceive<OsPacketBufMut, UdpRecvMeta>>,
    ) -> Result<usize, Error> {
        let mut count = out.remaining().min(self.max_batch);
        if count == 0 {
            return Ok(0);
        }

        for index in 0..count {
            if self.recv_buffers[index].is_some() {
                continue;
            }

            let Some(buffer) = self.rx_pool.allocate() else {
                count = index;
                break;
            };
            self.recv_buffers[index] = Some(buffer);
        }

        if count == 0 {
            return Ok(0);
        }

        for index in 0..count {
            let buffer = self.recv_buffers[index]
                .as_mut()
                .expect("prepared receive slot has a packet buffer");
            let iov = &mut self.recv_iovs[index];
            iov.iov_base = buffer.data_ptr().cast();
            iov.iov_len = buffer.data_capacity();
        }

        for hdr in &mut self.recv_hdrs[..count] {
            hdr.msg_hdr.msg_namelen =
                std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
            // The buffer is pinned in `self.recv_cmsgs[index]`; reset the
            // capacity so the kernel can write a fresh IP_PKTINFO /
            // IPV6_PKTINFO control message each call.
            hdr.msg_hdr.msg_controllen = RECV_CMSG_LEN;
            hdr.msg_hdr.msg_flags = 0;
        }

        // Loop on EINTR before reporting back-pressure: a signal arriving
        // between `recvmmsg` entering the kernel and any datagram being
        // delivered would otherwise make us return `Ok(0)` and force the
        // caller into a polling cycle for no reason.
        let received = loop {
            let result = unsafe {
                libc::recvmmsg(
                    self.raw_fd,
                    self.recv_hdrs.as_mut_ptr(),
                    count as libc::c_uint,
                    libc::MSG_DONTWAIT,
                    std::ptr::null_mut(),
                )
            };
            if result < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                if is_transient(&error) {
                    return Ok(0);
                }
                return Err(io_error_to_core(error));
            }
            break result;
        };

        let received = received as usize;
        let mut delivered = 0;
        let mut had_truncation = false;
        for index in 0..received {
            let hdr = &self.recv_hdrs[index];
            if hdr.msg_hdr.msg_flags & libc::MSG_TRUNC != 0 {
                had_truncation = true;
                continue;
            }

            // The kernel always fills `msg_name` with a properly-sized
            // AF_INET / AF_INET6 sockaddr for an AF_INET / AF_INET6 UDP
            // socket, so `socketaddr_from_raw` can only return `None` if
            // the kernel violated this invariant. Surface that as a hard
            // failure instead of silently dropping the packet.
            let source = unsafe {
                socketaddr_from_raw(
                    (&raw const self.recv_addrs[index]).cast(),
                    hdr.msg_hdr.msg_namelen,
                )
            }
            .expect("kernel filled recvmmsg sockaddr with an unexpected family");

            let destination = unsafe { parse_pktinfo_destination(&hdr.msg_hdr) };

            let len = hdr.msg_len as usize;
            let mut packet = self.recv_buffers[index]
                .take()
                .expect("received slot has a packet buffer");
            packet.set_received_len(len).map_err(|_| Error::Truncated)?;
            let meta = UdpRecvMeta {
                source,
                destination,
                ecn: None,
                len,
                gro_stride: None,
            };
            out.push(UdpReceive::new(packet, meta))
                .expect("RecvBatch had remaining capacity reserved before recvmmsg");
            delivered += 1;
        }

        if delivered == 0 && had_truncation {
            return Err(Error::Truncated);
        }
        Ok(delivered)
    }

    #[cfg(not(target_os = "linux"))]
    fn recv_impl(
        &mut self,
        out: &mut RecvBatch<UdpReceive<OsPacketBufMut, UdpRecvMeta>>,
    ) -> Result<usize, Error> {
        let mut delivered = 0;

        while out.remaining() > 0 {
            let Some(mut packet) = self.rx_pool.allocate() else {
                return Ok(delivered);
            };
            let capacity = packet.data_capacity();
            let data = unsafe { std::slice::from_raw_parts_mut(packet.data_ptr(), capacity) };
            match self.socket.recv_from(data) {
                Ok((len, source)) => {
                    packet.set_received_len(len).map_err(|_| Error::Truncated)?;
                    let meta = UdpRecvMeta {
                        source,
                        destination: None,
                        ecn: None,
                        len,
                        gro_stride: None,
                    };
                    out.push(UdpReceive::new(packet, meta))
                        .map_err(|_| Error::BatchFull)?;
                    delivered += 1;
                }
                Err(error) if is_transient(&error) => return Ok(delivered),
                Err(error) => return Err(io_error_to_core(error)),
            }
        }

        Ok(delivered)
    }
}

/// Readiness source backed by an OS UDP socket handle.
#[derive(Debug)]
pub struct OsReadinessSource {
    socket: StdUdpSocket,
}

impl OsReadinessSource {
    /// Creates a readiness source from a UDP socket handle.
    #[must_use]
    pub const fn new(socket: StdUdpSocket) -> Self {
        Self { socket }
    }
}

impl ReadinessSource for OsReadinessSource {
    fn wait(&mut self, timeout: Option<Duration>) -> Result<WaitOutcome, Error> {
        wait_for_readable(&self.socket, timeout)
    }

    fn wake_handle(&self) -> Option<WakeHandle<'_>> {
        wake_handle(&self.socket)
    }
}

#[cfg(unix)]
fn bind_udp_socket(addr: SocketAddr, reuse_port: bool) -> io::Result<StdUdpSocket> {
    if !reuse_port {
        return StdUdpSocket::bind(addr);
    }

    use std::os::fd::{FromRawFd, OwnedFd};

    let domain = match addr {
        SocketAddr::V4(_) => libc::AF_INET,
        SocketAddr::V6(_) => libc::AF_INET6,
    };
    let fd = unsafe { libc::socket(domain, libc::SOCK_DGRAM, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };

    set_close_on_exec(fd.as_raw_fd())?;
    set_reuse_addr(fd.as_raw_fd())?;
    set_reuse_port(fd.as_raw_fd())?;

    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let addr_len = sockaddr_from_socketaddr(&addr, &mut storage);
    let result = unsafe { libc::bind(fd.as_raw_fd(), (&raw const storage).cast(), addr_len) };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(StdUdpSocket::from(fd))
}

#[cfg(unix)]
fn set_close_on_exec(fd: std::os::fd::RawFd) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    let result = unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(unix))]
fn bind_udp_socket(addr: SocketAddr, reuse_port: bool) -> io::Result<StdUdpSocket> {
    if reuse_port {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "SO_REUSEPORT is not supported on this platform",
        ));
    }
    StdUdpSocket::bind(addr)
}

#[cfg(unix)]
fn set_reuse_addr(fd: std::os::fd::RawFd) -> io::Result<()> {
    let enabled: libc::c_int = 1;
    let result = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_REUSEADDR,
            (&raw const enabled).cast(),
            std::mem::size_of_val(&enabled) as libc::socklen_t,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(any(
    target_os = "android",
    target_os = "dragonfly",
    target_os = "freebsd",
    target_os = "ios",
    target_os = "linux",
    target_os = "macos",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "tvos",
    target_os = "watchos"
))]
fn set_reuse_port(fd: std::os::fd::RawFd) -> io::Result<()> {
    let enabled: libc::c_int = 1;
    let result = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_REUSEPORT,
            (&raw const enabled).cast(),
            std::mem::size_of_val(&enabled) as libc::socklen_t,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(all(
    unix,
    not(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "ios",
        target_os = "linux",
        target_os = "macos",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "tvos",
        target_os = "watchos"
    ))
))]
fn set_reuse_port(_fd: std::os::fd::RawFd) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "SO_REUSEPORT is not supported on this platform",
    ))
}

fn is_transient(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut | io::ErrorKind::Interrupted
    )
}

fn has_unsupported_tx_options<B>(tx: &UdpTransmit<B>) -> bool {
    tx.source_ip.is_some() || tx.ecn.is_some() || tx.gso_segment_size.is_some()
}

#[cfg(target_os = "linux")]
struct RecvSyscallState {
    addrs: Box<[libc::sockaddr_storage]>,
    iovs: Box<[libc::iovec]>,
    hdrs: Box<[libc::mmsghdr]>,
    cmsgs: Box<[[u8; RECV_CMSG_LEN]]>,
}

#[cfg(target_os = "linux")]
fn build_recv_state(batch: usize) -> RecvSyscallState {
    let mut recv_addrs: Box<[libc::sockaddr_storage]> =
        (0..batch).map(|_| unsafe { std::mem::zeroed() }).collect();
    let mut recv_iovs: Box<[libc::iovec]> = (0..batch)
        .map(|_| libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 0,
        })
        .collect();
    let mut recv_hdrs: Box<[libc::mmsghdr]> =
        (0..batch).map(|_| unsafe { std::mem::zeroed() }).collect();
    let mut recv_cmsgs: Box<[[u8; RECV_CMSG_LEN]]> =
        (0..batch).map(|_| [0u8; RECV_CMSG_LEN]).collect();

    for index in 0..batch {
        let cmsg_ptr = (&raw mut recv_cmsgs[index]).cast();
        let hdr = &mut recv_hdrs[index].msg_hdr;
        hdr.msg_name = (&raw mut recv_addrs[index]).cast();
        hdr.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        hdr.msg_iov = &raw mut recv_iovs[index];
        hdr.msg_iovlen = 1;
        hdr.msg_control = cmsg_ptr;
        hdr.msg_controllen = RECV_CMSG_LEN;
    }

    RecvSyscallState {
        addrs: recv_addrs,
        iovs: recv_iovs,
        hdrs: recv_hdrs,
        cmsgs: recv_cmsgs,
    }
}

#[cfg(target_os = "linux")]
struct SendSyscallState {
    addrs: Box<[libc::sockaddr_storage]>,
    iov_ranges: Box<[(usize, usize)]>,
    hdrs: Box<[libc::mmsghdr]>,
}

/// Enables `IP_PKTINFO` (and `IPV6_RECVPKTINFO`) so `recvmmsg` includes an
/// ancillary message naming the destination IP / arrival ifindex for each
/// datagram. Failures are non-fatal: a kernel/socket that does not support
/// PKTINFO simply leaves `UdpRecvMeta.destination = None`.
#[cfg(target_os = "linux")]
fn enable_pktinfo(socket: &StdUdpSocket) -> io::Result<()> {
    let enabled: libc::c_int = 1;
    let len = std::mem::size_of_val(&enabled) as libc::socklen_t;
    let ptr = (&raw const enabled).cast();
    // SAFETY: enabled is a stack int; setsockopt does not retain it.
    let v4 = unsafe {
        libc::setsockopt(socket.as_raw_fd(), libc::IPPROTO_IP, libc::IP_PKTINFO, ptr, len)
    };
    let v6 = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::IPPROTO_IPV6,
            libc::IPV6_RECVPKTINFO,
            ptr,
            len,
        )
    };
    // Tolerate ENOPROTOOPT / EINVAL (single-family socket); only surface
    // other errors.
    for rc in [v4, v6] {
        if rc != 0 {
            let err = io::Error::last_os_error();
            match err.raw_os_error() {
                Some(libc::ENOPROTOOPT) | Some(libc::EINVAL) => continue,
                _ => return Err(err),
            }
        }
    }
    Ok(())
}

/// Parses `IP_PKTINFO` / `IPV6_PKTINFO` ancillary data out of a finished
/// `recvmmsg` `msghdr` and returns the local IP that the datagram landed on.
/// Returns `None` if no PKTINFO control message was attached.
///
/// # Safety
/// `hdr.msg_control` must point to `hdr.msg_controllen` initialized bytes
/// produced by the kernel for this message.
#[cfg(target_os = "linux")]
unsafe fn parse_pktinfo_destination(hdr: &libc::msghdr) -> Option<IpAddr> {
    if hdr.msg_control.is_null() || hdr.msg_controllen == 0 {
        return None;
    }
    let mut cmsg = unsafe { libc::CMSG_FIRSTHDR(hdr) };
    while !cmsg.is_null() {
        let level = unsafe { (*cmsg).cmsg_level };
        let ty = unsafe { (*cmsg).cmsg_type };
        let data = unsafe { libc::CMSG_DATA(cmsg) };
        if level == libc::IPPROTO_IP && ty == libc::IP_PKTINFO {
            let info: libc::in_pktinfo =
                unsafe { std::ptr::read_unaligned(data.cast::<libc::in_pktinfo>()) };
            return Some(IpAddr::V4(Ipv4Addr::from(
                info.ipi_addr.s_addr.to_ne_bytes(),
            )));
        }
        if level == libc::IPPROTO_IPV6 && ty == libc::IPV6_PKTINFO {
            let info: libc::in6_pktinfo =
                unsafe { std::ptr::read_unaligned(data.cast::<libc::in6_pktinfo>()) };
            return Some(IpAddr::V6(Ipv6Addr::from(info.ipi6_addr.s6_addr)));
        }
        cmsg = unsafe { libc::CMSG_NXTHDR(hdr, cmsg) };
    }
    None
}

#[cfg(target_os = "linux")]
fn build_send_state(batch: usize) -> SendSyscallState {
    let tx_addrs: Box<[libc::sockaddr_storage]> =
        (0..batch).map(|_| unsafe { std::mem::zeroed() }).collect();
    let tx_iov_ranges: Box<[(usize, usize)]> = (0..batch).map(|_| (0, 0)).collect();
    let tx_hdrs: Box<[libc::mmsghdr]> = (0..batch).map(|_| unsafe { std::mem::zeroed() }).collect();
    SendSyscallState {
        addrs: tx_addrs,
        iov_ranges: tx_iov_ranges,
        hdrs: tx_hdrs,
    }
}

#[cfg(unix)]
fn sockaddr_from_socketaddr(
    addr: &SocketAddr,
    storage: &mut libc::sockaddr_storage,
) -> libc::socklen_t {
    match addr {
        SocketAddr::V4(v4) => {
            let sockaddr = storage as *mut _ as *mut libc::sockaddr_in;
            unsafe {
                *sockaddr = std::mem::zeroed();
                (*sockaddr).sin_family = libc::AF_INET as libc::sa_family_t;
                (*sockaddr).sin_port = v4.port().to_be();
                (*sockaddr).sin_addr.s_addr = u32::from_ne_bytes(v4.ip().octets());
            }
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t
        }
        SocketAddr::V6(v6) => {
            let sockaddr = storage as *mut _ as *mut libc::sockaddr_in6;
            unsafe {
                *sockaddr = std::mem::zeroed();
                (*sockaddr).sin6_family = libc::AF_INET6 as libc::sa_family_t;
                (*sockaddr).sin6_port = v6.port().to_be();
                (*sockaddr).sin6_addr.s6_addr = v6.ip().octets();
                (*sockaddr).sin6_flowinfo = v6.flowinfo();
                (*sockaddr).sin6_scope_id = v6.scope_id();
            }
            std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t
        }
    }
}

#[cfg(target_os = "linux")]
unsafe fn socketaddr_from_raw(
    sockaddr: *const libc::sockaddr,
    len: libc::socklen_t,
) -> Option<SocketAddr> {
    if sockaddr.is_null() {
        return None;
    }

    match unsafe { (*sockaddr).sa_family as libc::c_int } {
        libc::AF_INET if len as usize >= std::mem::size_of::<libc::sockaddr_in>() => {
            let sockaddr = unsafe { &*(sockaddr.cast::<libc::sockaddr_in>()) };
            Some(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::from(sockaddr.sin_addr.s_addr.to_ne_bytes()),
                u16::from_be(sockaddr.sin_port),
            )))
        }
        libc::AF_INET6 if len as usize >= std::mem::size_of::<libc::sockaddr_in6>() => {
            let sockaddr = unsafe { &*(sockaddr.cast::<libc::sockaddr_in6>()) };
            Some(SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::from(sockaddr.sin6_addr.s6_addr),
                u16::from_be(sockaddr.sin6_port),
                sockaddr.sin6_flowinfo,
                sockaddr.sin6_scope_id,
            )))
        }
        _ => None,
    }
}

#[cfg(target_os = "linux")]
fn configure_queue_affinity(
    socket: &StdUdpSocket,
    queue_affinity: QueueAffinity,
) -> io::Result<()> {
    let QueueAffinity::Core(cpu) = queue_affinity else {
        return Ok(());
    };

    let cpu: libc::c_int = cpu.try_into().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "SO_INCOMING_CPU value does not fit c_int",
        )
    })?;
    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_INCOMING_CPU,
            (&raw const cpu).cast(),
            std::mem::size_of_val(&cpu) as libc::socklen_t,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn configure_queue_affinity(
    _socket: &StdUdpSocket,
    _queue_affinity: QueueAffinity,
) -> io::Result<()> {
    Ok(())
}

fn io_error_to_core(error: io::Error) -> Error {
    match error.kind() {
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut | io::ErrorKind::Interrupted => {
            Error::WouldBlock
        }
        io::ErrorKind::InvalidInput => Error::OversizeForMtu,
        io::ErrorKind::AddrNotAvailable
        | io::ErrorKind::NetworkUnreachable
        | io::ErrorKind::HostUnreachable => Error::NoEgressRoute,
        io::ErrorKind::ConnectionRefused | io::ErrorKind::ConnectionReset => {
            Error::Device(DeviceError::with_source(DeviceErrorKind::FdClosed, error))
        }
        _ => Error::Device(DeviceError::with_source(DeviceErrorKind::Backend, error)),
    }
}

#[cfg(unix)]
fn wait_for_readable(
    socket: &StdUdpSocket,
    timeout: Option<Duration>,
) -> Result<WaitOutcome, Error> {
    #[repr(C)]
    struct PollFd {
        fd: c_int,
        events: c_short,
        revents: c_short,
    }

    const POLLIN: c_short = 0x0001;

    unsafe extern "C" {
        fn poll(fds: *mut PollFd, nfds: Nfds, timeout: c_int) -> c_int;
    }

    let mut fd = PollFd {
        fd: socket.as_raw_fd(),
        events: POLLIN,
        revents: 0,
    };
    let timeout_ms = timeout_to_poll_ms(timeout);

    // SAFETY: `fd` points to a valid single-element pollfd array for the
    // duration of the call. `poll` does not retain the pointer after returning.
    let result = unsafe { poll(&mut fd, 1 as Nfds, timeout_ms) };
    match result {
        value if value > 0 => Ok(WaitOutcome::Ready),
        0 => Ok(WaitOutcome::Timeout),
        _ => {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                Ok(WaitOutcome::Spurious)
            } else {
                Err(io_error_to_core(error))
            }
        }
    }
}

#[cfg(not(unix))]
fn wait_for_readable(
    _socket: &StdUdpSocket,
    timeout: Option<Duration>,
) -> Result<WaitOutcome, Error> {
    if let Some(timeout) = timeout {
        std::thread::sleep(timeout);
        Ok(WaitOutcome::Timeout)
    } else {
        Ok(WaitOutcome::Spurious)
    }
}

#[cfg(unix)]
fn wake_handle(socket: &StdUdpSocket) -> Option<WakeHandle<'_>> {
    Some(WakeHandle::from_fd(socket.as_fd()))
}

#[cfg(not(unix))]
fn wake_handle(_socket: &StdUdpSocket) -> Option<WakeHandle<'_>> {
    None
}

fn timeout_to_poll_ms(timeout: Option<Duration>) -> i32 {
    match timeout {
        None => -1,
        Some(timeout) if timeout.is_zero() => 0,
        Some(timeout) => timeout.as_millis().try_into().unwrap_or(i32::MAX).max(1),
    }
}

#[cfg(test)]
mod tests {
    use core::num::NonZeroU16;

    use std::net::{Ipv4Addr, SocketAddrV4};
    use std::time::Duration;

    use fast_socket_rs::{BufferLayout, PacketBufferMut};

    use super::*;

    #[test]
    fn os_udp_socket_sends_and_receives_payload() {
        let mut receiver =
            OsUdpSocketBuilder::new(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0).into())
                .buffer_layout(BufferLayout::with_headroom_and_tailroom(256, 0, 0))
                .mtu(256)
                .bind()
                .unwrap();
        let receive_addr = receiver.local_addr().unwrap();

        let mut sender = OsUdpSocketBuilder::new(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0).into())
            .buffer_layout(BufferLayout::with_headroom_and_tailroom(256, 0, 0))
            .mtu(256)
            .bind()
            .unwrap();
        let sender_addr = sender.local_addr().unwrap();

        let packet = tx_packet(&mut sender, b"hello from os");
        let mut tx = [TxSlot::Ready(UdpTransmit::new(packet, receive_addr))];
        assert_eq!(sender.send(&mut tx).unwrap(), 1);
        assert!(tx[0].is_taken());
        assert_eq!(sender.drain_tx_completions().unwrap(), 0);

        let mut rx = RecvBatch::with_capacity(4);
        for _ in 0..20 {
            if receiver.recv(&mut rx).unwrap() > 0 {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        assert_eq!(rx.len(), 1);
        let received = &rx.as_slice()[0];
        assert_eq!(received.packet.as_slice(), b"hello from os");
        assert_eq!(received.meta.source, sender_addr);
        // The destination should be the local address the kernel routed the
        // packet to. On Linux + IP_PKTINFO that is the bound 127.0.0.1; on
        // platforms without PKTINFO support the field remains None.
        match received.meta.destination {
            Some(IpAddr::V4(addr)) => assert_eq!(addr, Ipv4Addr::LOCALHOST),
            Some(other) => panic!("unexpected destination family: {other:?}"),
            None => {}
        }
        assert_eq!(received.meta.len, b"hello from os".len());
    }

    #[test]
    fn os_udp_socket_rejects_oversize_transmit_without_consuming_slot() {
        let mut sender = OsUdpSocketBuilder::new(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0).into())
            .buffer_layout(BufferLayout::with_headroom_and_tailroom(256, 0, 0))
            .mtu(4)
            .bind()
            .unwrap();
        let destination = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9).into();
        let packet = tx_packet(&mut sender, b"too large");
        let mut tx = [TxSlot::Ready(UdpTransmit::new(packet, destination))];

        let error = sender
            .send(&mut tx)
            .expect_err("oversize packet is rejected");
        assert_eq!(error.accepted, 0);
        assert!(matches!(error.kind, Error::OversizeForMtu));
        assert!(tx[0].is_ready());
    }

    #[test]
    fn os_udp_socket_rejects_unsupported_transmit_options() {
        let mut sender = OsUdpSocketBuilder::new(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0).into())
            .buffer_layout(BufferLayout::with_headroom_and_tailroom(256, 0, 0))
            .mtu(256)
            .bind()
            .unwrap();
        let destination = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9).into();
        let packet = tx_packet(&mut sender, b"segmented");
        let mut transmit = UdpTransmit::new(packet, destination);
        transmit.gso_segment_size = NonZeroU16::new(4);
        let mut tx = [TxSlot::Ready(transmit)];

        let error = sender
            .send(&mut tx)
            .expect_err("unsupported metadata is rejected");
        assert_eq!(error.accepted, 0);
        assert!(matches!(error.kind, Error::InvalidPacket));
        assert!(tx[0].is_ready());
    }

    #[test]
    fn os_udp_socket_accepts_valid_prefix_before_rejecting_bad_slot() {
        let mut sender = OsUdpSocketBuilder::new(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0).into())
            .buffer_layout(BufferLayout::with_headroom_and_tailroom(256, 0, 0))
            .mtu(4)
            .bind()
            .unwrap();
        let destination = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9).into();
        let first = tx_packet(&mut sender, b"ok");
        let second = tx_packet(&mut sender, b"too large");
        let mut tx = [
            TxSlot::Ready(UdpTransmit::new(first, destination)),
            TxSlot::Ready(UdpTransmit::new(second, destination)),
        ];

        let error = sender.send(&mut tx).expect_err("second packet is rejected");
        assert_eq!(error.accepted, 1);
        assert!(matches!(error.kind, Error::OversizeForMtu));
        assert!(tx[0].is_taken());
        assert!(tx[1].is_ready());
    }

    #[test]
    fn os_udp_socket_exposes_separate_receive_and_transmit_pools() {
        let mut socket = OsUdpSocketBuilder::new(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0).into())
            .rx_buffer_layout(BufferLayout::with_headroom_and_tailroom(128, 4, 8))
            .tx_buffer_layout(BufferLayout::with_headroom_and_tailroom(512, 64, 16))
            .bind()
            .unwrap();

        assert_eq!(socket.rx_pool().layout().payload_capacity(), 128);
        assert_eq!(socket.rx_pool().layout().headroom(), 4);
        assert_eq!(socket.tx_pool().layout().payload_capacity(), 512);
        assert_eq!(socket.tx_pool().layout().headroom(), 64);

        let mut tx_buffer = socket.tx_pool_mut().allocate().unwrap();
        tx_buffer.extend_from_slice(b"tx").unwrap();
        assert_eq!(tx_buffer.headroom(), 64);
    }

    #[cfg(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "ios",
        target_os = "linux",
        target_os = "macos",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "tvos",
        target_os = "watchos"
    ))]
    #[test]
    fn os_udp_socket_reuse_port_allows_shared_bind() {
        let first = OsUdpSocketBuilder::new(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0).into())
            .reuse_port(true)
            .bind()
            .unwrap();
        let addr = first.local_addr().unwrap();

        let second = OsUdpSocketBuilder::new(addr)
            .reuse_port(true)
            .bind()
            .unwrap();

        assert_eq!(second.local_addr().unwrap(), addr);
    }

    #[test]
    fn os_udp_socket_reports_configured_identity() {
        let socket = OsUdpSocketBuilder::new(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0).into())
            .if_index(IfIndex::new(12))
            .queue_id(QueueId::new(3))
            .bind()
            .unwrap();

        assert_eq!(socket.if_index(), Some(IfIndex::new(12)));
        assert_eq!(socket.socket_id(), SocketId::new(3));
    }

    #[test]
    fn os_readiness_driver_times_out() {
        let socket = OsUdpSocketBuilder::new(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0).into())
            .bind()
            .unwrap();
        let driver = socket.driver().source().socket.try_clone().unwrap();
        driver.set_nonblocking(true).unwrap();

        let mut source = OsReadinessSource::new(driver);
        assert_eq!(
            source.wait(Some(Duration::from_millis(1))).unwrap(),
            WaitOutcome::Timeout
        );
    }

    fn tx_packet(socket: &mut OsUdpSocket, bytes: &[u8]) -> OsPacketBuf {
        let mut packet = socket.tx_pool_mut().allocate().unwrap();
        packet.extend_from_slice(bytes).unwrap();
        packet.freeze()
    }
}
