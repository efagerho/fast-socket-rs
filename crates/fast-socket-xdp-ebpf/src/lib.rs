//! Host-facing eBPF program bytes and map contract constants.
#![no_std]

/// Max `(rx_queue_index -> AF_XDP fd)` entries in `XSKMAP`.
///
/// Modern multi-queue NICs (mlx5, ice, mlx4) routinely expose more than 64
/// combined queues; the previous cap silently truncated registration for
/// any queue id at or above 64. 256 covers every NIC we expect to encounter
/// in benchmarks; bumping it costs 256 × 4 = 1 KiB of BPF map memory per
/// loaded program.
pub const MAX_QUEUES: u32 = 256;

/// Number of UDP destination-port membership slots in `BOUND_PORTS`.
pub const BOUND_PORTS_LEN: u32 = 1 << 16;

/// Number of entries in `BOUND_PORT_COUNT`.
pub const BOUND_PORT_COUNT_LEN: u32 = 1;

/// Backwards-compatible alias for users that name the port-map capacity.
pub const MAX_BOUND_PORTS: u32 = BOUND_PORTS_LEN;

/// Number of entries in `BOUND_PORT_RANGE`.
///
/// Slot [`BOUND_PORT_RANGE_START`] stores the inclusive first UDP destination
/// port. Slot [`BOUND_PORT_RANGE_END`] stores the inclusive last UDP
/// destination port. The all-zero range is treated as disabled so a freshly
/// loaded program passes all traffic to the OS until userspace configures it.
pub const BOUND_PORT_RANGE_LEN: u32 = 2;

/// Index of the inclusive start port in `BOUND_PORT_RANGE`.
pub const BOUND_PORT_RANGE_START: u32 = 0;

/// Index of the inclusive end port in `BOUND_PORT_RANGE`.
pub const BOUND_PORT_RANGE_END: u32 = 1;

/// XDP program name for the per-port membership array filter.
pub const BOUND_PORTS_PROGRAM: &str = "fast_socket_xdp";

/// XDP program name for the inclusive UDP destination-port range filter.
pub const PORT_RANGE_PROGRAM: &str = "fast_socket_xdp_port_range";

/// Number of drop-reason buckets in `DROP_COUNTERS`. Each slot holds a
/// `u64` counter incremented every time the eBPF program drops a packet
/// for the corresponding reason. Userspace reads these as a coarse metric
/// to confirm that XDP filtering is actually doing work and to detect
/// queue-registration mistakes.
pub const DROP_COUNTERS_LEN: u32 = 2;

/// Drop reason: packet arrived on a queue with no AF_XDP socket registered
/// in `XSKMAP[rx_queue_index]`. Indicates either a partially registered
/// NIC or a queue id beyond the user's registration. The previous behavior
/// was `XDP_PASS`, which silently let those packets reach the kernel
/// stack; with the new policy they are dropped and counted here.
pub const DROP_REASON_XSKMAP_MISS: u32 = 0;

/// Drop reason: packet was destined for a bound UDP port but the AF_XDP
/// `redirect()` action returned an error other than "miss" (typically
/// rare; included for completeness so the counter sum equals every
/// dropped packet).
pub const DROP_REASON_REDIRECT_ERROR: u32 = 1;

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
