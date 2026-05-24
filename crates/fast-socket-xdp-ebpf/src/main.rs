//! XDP redirect program for AF_XDP raw sockets.
//!
//! The program redirects IPv4 and IPv6 Ethernet frames to the AF_XDP socket
//! registered in `XSKMAP[rx_queue_index]`. When userspace binds UDP ports, only
//! matching IPv4 UDP packets are redirected and unrelated traffic stays on the
//! kernel path.

#![no_std]
#![no_main]

use aya_ebpf::{
    bindings::xdp_action,
    macros::{map, xdp},
    maps::{Array, XskMap},
    programs::XdpContext,
};
use fast_socket_xdp_ebpf::{BOUND_PORT_COUNT_LEN, BOUND_PORTS_LEN, MAX_QUEUES};

/// `rx_queue_index -> AF_XDP socket fd`. Userspace inserts after `bind(2)`.
#[map]
static XSKMAP: XskMap = XskMap::with_max_entries(MAX_QUEUES, 0);

/// UDP destination ports redirected while `BOUND_PORT_COUNT[0]` is non-zero.
#[map]
static BOUND_PORTS: Array<u8> = Array::with_max_entries(BOUND_PORTS_LEN, 0);

/// Number of non-zero entries in `BOUND_PORTS`.
#[map]
static BOUND_PORT_COUNT: Array<u32> = Array::with_max_entries(BOUND_PORT_COUNT_LEN, 0);

const ETHER_TYPE_OFFSET: usize = 12;
const VLAN_INNER_ETHER_TYPE_OFFSET: usize = 16;
const ETHERTYPE_IPV4: u16 = 0x0800;
const ETHERTYPE_IPV6: u16 = 0x86dd;
const ETHERTYPE_VLAN: u16 = 0x8100;
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
    }

    if ethertype != ETHERTYPE_IPV4 && ethertype != ETHERTYPE_IPV6 {
        return Ok(xdp_action::XDP_PASS);
    }

    if has_bound_ports() {
        if ethertype != ETHERTYPE_IPV4 {
            return Ok(xdp_action::XDP_PASS);
        }
        let Some(destination_port) = ipv4_udp_destination_port(ctx, l2_len)? else {
            return Ok(xdp_action::XDP_PASS);
        };
        if !is_bound_port(destination_port) {
            return Ok(xdp_action::XDP_PASS);
        }
    }

    let queue_id = unsafe { (*ctx.ctx).rx_queue_index };
    Ok(XSKMAP.redirect(queue_id, 0).unwrap_or(xdp_action::XDP_PASS))
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
    let version_ihl = unsafe { read_u8(ctx, l2_len)? };
    if version_ihl >> 4 != 4 {
        return Ok(None);
    }

    let ihl = usize::from(version_ihl & 0x0f) * 4;
    if ihl < IPV4_MIN_HEADER_LEN {
        return Ok(None);
    }

    let fragment = unsafe { read_u16_be(ctx, l2_len + IPV4_FRAGMENT_OFFSET)? };
    if fragment & IPV4_FRAGMENT_MASK != 0 {
        return Ok(None);
    }

    let protocol = unsafe { read_u8(ctx, l2_len + IPV4_PROTOCOL_OFFSET)? };
    if protocol != IPV4_PROTOCOL_UDP {
        return Ok(None);
    }

    let udp_offset = l2_len + ihl;
    unsafe { read_u8(ctx, udp_offset + UDP_HEADER_LEN - 1)? };
    Ok(Some(unsafe {
        read_u16_be(ctx, udp_offset + UDP_DESTINATION_PORT_OFFSET)?
    }))
}

#[inline(always)]
unsafe fn ptr_at<T>(ctx: &XdpContext, offset: usize) -> Result<*const T, ()> {
    let start = ctx.data();
    let end = ctx.data_end();
    let size = core::mem::size_of::<T>();
    if start + offset + size > end {
        return Err(());
    }
    Ok((start + offset) as *const T)
}

#[inline(always)]
unsafe fn read_u8(ctx: &XdpContext, offset: usize) -> Result<u8, ()> {
    Ok(unsafe { *ptr_at::<u8>(ctx, offset)? })
}

#[inline(always)]
unsafe fn read_u16_be(ctx: &XdpContext, offset: usize) -> Result<u16, ()> {
    let high = unsafe { *ptr_at::<u8>(ctx, offset)? } as u16;
    let low = unsafe { *ptr_at::<u8>(ctx, offset + 1)? } as u16;
    Ok((high << 8) | low)
}

#[cfg(target_arch = "bpf")]
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}
