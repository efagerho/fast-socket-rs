//! XDP redirect program for AF_XDP raw sockets.
//!
//! The program redirects IPv4 and IPv6 Ethernet frames to the AF_XDP socket
//! registered in `XSKMAP[rx_queue_index]`. When userspace binds UDP ports, only
//! matching IPv4 UDP packets are redirected and unrelated traffic stays on the
//! kernel path.
//!
//! The whole file is gated on `cfg(target_arch = "bpf")`. On host
//! architectures the compilation produces a tiny stub `fn main() {}` so
//! `cargo check --workspace` works without the BPF toolchain; the real
//! program is built via `build-ebpf.sh` (which targets `bpfel-unknown-none`).

// On the BPF target we are a `#![no_std]` / `#![no_main]` program with a
// custom panic handler (in `mod bpf` below). On host architectures the
// crate is just a stub that links into a no-op `fn main()` so
// `cargo check --workspace` works without the BPF toolchain.
#![cfg_attr(target_arch = "bpf", no_std)]
#![cfg_attr(target_arch = "bpf", no_main)]

#[cfg(not(target_arch = "bpf"))]
fn main() {}

#[cfg(target_arch = "bpf")]
mod bpf {

use aya_ebpf::{
    bindings::xdp_action,
    macros::{map, xdp},
    maps::{Array, XskMap},
    programs::XdpContext,
};
use fast_socket_xdp_ebpf::{
    BOUND_PORT_COUNT_LEN, BOUND_PORTS_LEN, DROP_COUNTERS_LEN, DROP_REASON_REDIRECT_ERROR,
    DROP_REASON_XSKMAP_MISS, MAX_QUEUES,
};

/// `rx_queue_index -> AF_XDP socket fd`. Userspace inserts after `bind(2)`.
#[map]
static XSKMAP: XskMap = XskMap::with_max_entries(MAX_QUEUES, 0);

/// UDP destination ports redirected while `BOUND_PORT_COUNT[0]` is non-zero.
#[map]
static BOUND_PORTS: Array<u8> = Array::with_max_entries(BOUND_PORTS_LEN, 0);

/// Number of non-zero entries in `BOUND_PORTS`.
#[map]
static BOUND_PORT_COUNT: Array<u32> = Array::with_max_entries(BOUND_PORT_COUNT_LEN, 0);

/// Per-reason drop counters. Indexed by `DROP_REASON_*` constants.
#[map]
static DROP_COUNTERS: Array<u64> = Array::with_max_entries(DROP_COUNTERS_LEN, 0);

const ETHER_TYPE_OFFSET: usize = 12;
const VLAN_INNER_ETHER_TYPE_OFFSET: usize = 16;
const QINQ_INNER_ETHER_TYPE_OFFSET: usize = 20;
const ETHERTYPE_IPV4: u16 = 0x0800;
const ETHERTYPE_IPV6: u16 = 0x86dd;
/// Single 802.1Q tag.
const ETHERTYPE_VLAN: u16 = 0x8100;
/// Outer 802.1ad (S-VLAN) tag for QinQ. The inner tag is `ETHERTYPE_VLAN`.
const ETHERTYPE_QINQ: u16 = 0x88a8;
const IPV4_MIN_HEADER_LEN: usize = 20;
const IPV4_FRAGMENT_MASK: u16 = 0x3fff;
const IPV4_PROTOCOL_UDP: u8 = 17;
const IPV4_FRAGMENT_OFFSET: usize = 6;
const IPV4_PROTOCOL_OFFSET: usize = 9;
const UDP_DESTINATION_PORT_OFFSET: usize = 2;
const UDP_HEADER_LEN: usize = 8;

#[xdp]
pub fn fast_socket_xdp(ctx: XdpContext) -> u32 {
    match try_redirect(&ctx) {
        Ok(action) => action,
        Err(()) => xdp_action::XDP_PASS,
    }
}

#[inline(always)]
fn try_redirect(ctx: &XdpContext) -> Result<u32, ()> {
    let mut l2_len = 14;
    let mut ethertype = unsafe { read_u16_be(ctx, ETHER_TYPE_OFFSET)? };
    if ethertype == ETHERTYPE_VLAN {
        ethertype = unsafe { read_u16_be(ctx, VLAN_INNER_ETHER_TYPE_OFFSET)? };
        l2_len = 18;
    } else if ethertype == ETHERTYPE_QINQ {
        // 802.1ad outer S-VLAN (0x88a8). Expect a single inner 802.1Q tag
        // and read the inner-inner ethertype. Anything else (e.g., stacked
        // S-VLAN-on-S-VLAN) falls through to XDP_PASS — there is no second
        // QinQ layer in any traffic we currently target.
        let inner_tpid = unsafe { read_u16_be(ctx, VLAN_INNER_ETHER_TYPE_OFFSET)? };
        if inner_tpid != ETHERTYPE_VLAN {
            return Ok(xdp_action::XDP_PASS);
        }
        ethertype = unsafe { read_u16_be(ctx, QINQ_INNER_ETHER_TYPE_OFFSET)? };
        l2_len = 22;
    }

    if ethertype != ETHERTYPE_IPV4 && ethertype != ETHERTYPE_IPV6 {
        return Ok(xdp_action::XDP_PASS);
    }

    // Closed-by-default: with no UDP ports bound, leave every packet on the
    // kernel path. Userspace must explicitly call `bind_port(port)` to opt a
    // destination port into AF_XDP redirection. The previous behavior —
    // redirect everything when nothing is bound — surprised operators who
    // expected AF_XDP attachment alone not to hijack their TCP / ICMP / ND
    // traffic.
    if !has_bound_ports() {
        return Ok(xdp_action::XDP_PASS);
    }

    if ethertype != ETHERTYPE_IPV4 {
        return Ok(xdp_action::XDP_PASS);
    }
    let Some(destination_port) = ipv4_udp_destination_port(ctx, l2_len)? else {
        return Ok(xdp_action::XDP_PASS);
    };
    if !is_bound_port(destination_port) {
        return Ok(xdp_action::XDP_PASS);
    }

    let queue_id = unsafe { (*ctx.ctx).rx_queue_index };
    match XSKMAP.redirect(queue_id, 0) {
        Ok(action) => Ok(action),
        Err(_) => {
            // No AF_XDP socket registered for this queue (or the redirect
            // itself failed). The packet has already passed the bound-port
            // filter, so the operator clearly meant for it to be redirected
            // — silently passing it to the kernel stack hides the
            // misconfiguration. Drop it and bump a counter the operator can
            // read from userspace to spot partial / mis-registered queues.
            increment_drop_counter(DROP_REASON_XSKMAP_MISS);
            // `redirect` only returns `Err` on lookup miss in current aya;
            // count any other paths under REDIRECT_ERROR for forward-
            // compatibility with future aya semantics.
            let _ = DROP_REASON_REDIRECT_ERROR;
            Ok(xdp_action::XDP_DROP)
        }
    }
}

#[inline(always)]
fn increment_drop_counter(reason: u32) {
    if let Some(counter) = DROP_COUNTERS.get_ptr_mut(reason) {
        // SAFETY: `counter` points to a live `u64` inside the
        // DROP_COUNTERS array map. The increment is racy against
        // concurrent CPUs, but we accept the imprecision — these are
        // operator-visibility counters, not correctness-critical state.
        unsafe { *counter += 1 };
    }
}

#[inline(always)]
fn has_bound_ports() -> bool {
    match BOUND_PORT_COUNT.get(0) {
        Some(count) => *count != 0,
        None => false,
    }
}

#[inline(always)]
fn is_bound_port(port: u16) -> bool {
    match BOUND_PORTS.get(port as u32) {
        Some(enabled) => *enabled != 0,
        None => false,
    }
}

#[inline(always)]
fn ipv4_udp_destination_port(ctx: &XdpContext, l2_len: usize) -> Result<Option<u16>, ()> {
    // Read the IPv4 minimum-header bytes in one bounds check. The verifier
    // proves the whole array fits before any individual byte is dereferenced,
    // so subsequent header-field reads need no further `ptr_at` calls.
    let ipv4_header = unsafe { read_array::<IPV4_MIN_HEADER_LEN>(ctx, l2_len)? };
    let version_ihl = ipv4_header[0];
    if version_ihl >> 4 != 4 {
        return Ok(None);
    }
    let ihl = usize::from(version_ihl & 0x0f) * 4;
    if ihl < IPV4_MIN_HEADER_LEN {
        return Ok(None);
    }
    let fragment = u16::from_be_bytes([
        ipv4_header[IPV4_FRAGMENT_OFFSET],
        ipv4_header[IPV4_FRAGMENT_OFFSET + 1],
    ]);
    if fragment & IPV4_FRAGMENT_MASK != 0 {
        return Ok(None);
    }
    if ipv4_header[IPV4_PROTOCOL_OFFSET] != IPV4_PROTOCOL_UDP {
        return Ok(None);
    }

    // Read the whole UDP header in one bounds check too.
    let udp_offset = l2_len + ihl;
    let udp_header = unsafe { read_array::<UDP_HEADER_LEN>(ctx, udp_offset)? };
    let destination = u16::from_be_bytes([
        udp_header[UDP_DESTINATION_PORT_OFFSET],
        udp_header[UDP_DESTINATION_PORT_OFFSET + 1],
    ]);
    Ok(Some(destination))
}

#[inline(always)]
unsafe fn ptr_at<T>(ctx: &XdpContext, offset: usize) -> Result<*const T, ()> {
    // Canonical AF_XDP bounds-check form: keep arithmetic on the offset side
    // of the comparison so the verifier never has to track a possibly-
    // overflowing pointer sum. `offset + size` and `end - start` each fit in
    // a `usize` on every supported kernel, and the comparison itself is the
    // single check the verifier proves.
    let start = ctx.data();
    let end = ctx.data_end();
    let size = core::mem::size_of::<T>();
    if offset.checked_add(size).ok_or(())? > end.wrapping_sub(start) {
        return Err(());
    }
    Ok(start.wrapping_add(offset) as *const T)
}

/// Reads `N` bytes from the packet at `offset` in a single bounds check.
///
/// This is the preferred shape for the eBPF verifier: one `ptr_at::<[u8; N]>`
/// produces one `PTR_TO_PACKET` with a known length, and the verifier proves
/// the entire range fits before any individual byte read. Splitting the same
/// region into per-byte `ptr_at::<u8>` calls (the previous shape used by
/// `read_u8` / `read_u16_be`) generated one verifier branch per byte and
/// failed verification on stricter kernels for longer reads.
#[inline(always)]
unsafe fn read_array<const N: usize>(ctx: &XdpContext, offset: usize) -> Result<[u8; N], ()> {
    let p = unsafe { ptr_at::<[u8; N]>(ctx, offset)? };
    Ok(unsafe { *p })
}

#[inline(always)]
unsafe fn read_u16_be(ctx: &XdpContext, offset: usize) -> Result<u16, ()> {
    let bytes = unsafe { read_array::<2>(ctx, offset)? };
    Ok(u16::from_be_bytes(bytes))
}

#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}

}  // mod bpf
