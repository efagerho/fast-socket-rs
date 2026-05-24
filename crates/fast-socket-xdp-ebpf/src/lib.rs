//! Host-facing eBPF program bytes and map contract constants.
#![no_std]

/// Max `(rx_queue_index -> AF_XDP fd)` entries in `XSKMAP`.
pub const MAX_QUEUES: u32 = 64;

/// Number of UDP destination-port membership slots in `BOUND_PORTS`.
pub const BOUND_PORTS_LEN: u32 = 1 << 16;

/// Number of entries in `BOUND_PORT_COUNT`.
pub const BOUND_PORT_COUNT_LEN: u32 = 1;

/// Backwards-compatible alias for users that name the port-map capacity.
pub const MAX_BOUND_PORTS: u32 = BOUND_PORTS_LEN;

/// Number of drop-reason buckets in `DROP_COUNTERS`.
pub const DROP_COUNTERS_LEN: u32 = 2;

/// IPv4 UDP with IP options.
pub const DROP_REASON_UDP_OPTIONS: u32 = 0;

/// IPv4 UDP fragment.
pub const DROP_REASON_UDP_FRAGMENT: u32 = 1;

/// 8-byte-aligned wrapper for embedded BPF object bytes.
#[repr(C, align(8))]
pub struct Aligned<Bytes: ?Sized>(pub Bytes);

impl<Bytes: ?Sized> core::ops::Deref for Aligned<Bytes> {
    type Target = Bytes;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Pre-built BPF object. Rebuild with `build-ebpf.sh` after editing `main.rs`.
#[cfg(all(target_os = "linux", not(target_arch = "bpf")))]
pub static FAST_SOCKET_XDP_EBPF_PROGRAM: &Aligned<[u8]> = &Aligned(*include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fast-socket-xdp-prog"
)));
