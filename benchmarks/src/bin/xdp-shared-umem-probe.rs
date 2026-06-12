//! Hardware probe for `RawXdpSocket::new_shared_umem`.
//!
//! Binds two AF_XDP sockets to two queues of one device over a **single shared
//! UMEM**: member 0 registers the UMEM (`XDP_UMEM_REG`) and binds as the owner,
//! member 1 binds with `XDP_SHARED_UMEM` against the owner's fd. This validates
//! the shared-UMEM bind sequence against the real driver — the load-bearing
//! primitive for aggregate sockets — without needing the full aggregate.
//!
//! Usage: `sudo xdp-shared-umem-probe <iface> [queue_a] [queue_b]`
//! (defaults: queue_a=0, queue_b=1). Requires CAP_NET_ADMIN.

use std::error::Error;

use fast_socket_rs::HugePageSize;
use fast_socket_xdp_rs::internals::{RawXdpSocket, Umem};
use fast_socket_xdp_rs::{
    AttachMode, RingSizes, XdpMode, XdpProgramHandle, numa_node_for_interface,
    resolve_xdp_queue_slot, xdp_queue_slots_for_interface,
};

type BoxError = Box<dyn Error + Send + Sync>;

const FRAME_SIZE: u32 = 4096;
const FRAME_COUNT: u32 = 4096;

fn main() -> Result<(), BoxError> {
    let mut args = std::env::args().skip(1);
    let iface = args
        .next()
        .ok_or("usage: xdp-shared-umem-probe <iface> [queue_a] [queue_b]")?;
    let queue_a: u32 = args.next().map(|s| s.parse()).transpose()?.unwrap_or(0);
    let queue_b: u32 = args.next().map(|s| s.parse()).transpose()?.unwrap_or(1);
    let mode = match args.next().as_deref() {
        Some("copy") => XdpMode::Copy,
        Some("zerocopy") => XdpMode::ZeroCopy,
        Some("auto") | None => XdpMode::Auto,
        Some(other) => return Err(format!("unknown mode {other}").into()),
    };
    println!("requested mode: {mode:?}");

    let slots = xdp_queue_slots_for_interface(&iface)?;
    println!("{iface}: {} XDP queue slot(s)", slots.len());
    if queue_a == queue_b {
        return Err("queue_a and queue_b must differ for a shared-UMEM probe".into());
    }
    let slot_a = resolve_xdp_queue_slot(&iface, fast_socket_rs::QueueId::new(queue_a))?;
    let slot_b = resolve_xdp_queue_slot(&iface, fast_socket_rs::QueueId::new(queue_b))?;
    let numa = numa_node_for_interface(&iface)?;
    println!(
        "binding owner=queue {} shared=queue {} on ifindex {} (NUMA node {})",
        queue_a,
        queue_b,
        slot_a.ifindex.get(),
        numa.get()
    );

    // One UMEM shared by both members. Frames 0..N/2 prefill the owner's FILL
    // ring, N/2..N prefill the shared member's — disjoint so neither can hand
    // the same frame to the kernel twice.
    let mut umem = Umem::new_on_numa_node(FRAME_SIZE, FRAME_COUNT, HugePageSize::Size4K, numa)?;
    let half = FRAME_COUNT / 2;
    let prefill_cap = RingSizes::default().fill.min(half);
    let owner_prefill: Vec<u64> = (0..prefill_cap).map(|i| umem.frame_offset(i)).collect();
    let shared_prefill: Vec<u64> = (half..half + prefill_cap)
        .map(|i| umem.frame_offset(i))
        .collect();

    // Attach the redirect program to the interface once (mirrors the real
    // factory: one program per ifindex, each member registered in
    // XSKMAP[queue]). Native-mode binding on `ice` needs the program present.
    let program = XdpProgramHandle::load(slot_a.ifindex.get(), AttachMode::Default, None)?;
    println!("  XDP program attached to ifindex {}", slot_a.ifindex.get());

    let owner = RawXdpSocket::new(
        slot_a.ifindex.get(),
        queue_a,
        &mut umem,
        RingSizes::default(),
        mode,
        owner_prefill,
    )?;
    println!(
        "  owner bound: fd={} queue={}",
        owner.fd(),
        owner.queue_id()
    );

    let shared = RawXdpSocket::new_shared_umem(
        slot_b.ifindex.get(),
        queue_b,
        owner.fd(),
        RingSizes::default(),
        mode,
        shared_prefill,
    )?;
    println!(
        "  shared bound: fd={} queue={} (XDP_SHARED_UMEM -> owner fd {})",
        shared.fd(),
        shared.queue_id(),
        owner.fd()
    );
    drop(program);

    // Exercise the busy-poll setsockopts on both members too (best-effort:
    // SO_PREFER_BUSY_POLL / SO_BUSY_POLL_BUDGET need CAP_NET_ADMIN).
    for (label, sock) in [("owner", &owner), ("shared", &shared)] {
        match sock.configure_busy_poll(20, 64) {
            Ok(()) => println!("  {label}: busy-poll configured (20us, budget 64)"),
            Err(error) => println!("  {label}: busy-poll setsockopt failed: {error}"),
        }
    }

    println!("OK: shared-UMEM bind succeeded on real hardware");
    Ok(())
}
