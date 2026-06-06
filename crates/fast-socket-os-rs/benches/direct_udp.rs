//! Bench: many UDP datagrams in flight at once, measuring per-packet cost
//! through the `OsUdpSocket` send + recv path. The previous version did one
//! round trip at a time with `thread::sleep(50µs)` between calls, which
//! measured the sleep granularity (~1ms kernel HZ on most boxes) rather than
//! the socket path. The current version drains as many datagrams as possible
//! per syscall, keeps a bounded number of datagrams in flight so loopback UDP
//! drops do not dominate the measurement, and fails quickly if the socket path
//! stops making progress.

use std::hint::black_box;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::thread;
use std::time::{Duration, Instant};

use fast_socket_os_rs::{OsPacketBuf, OsUdpSocket, OsUdpSocketBuilder};
use fast_socket_rs::{
    BufferLayout, BufferPool, PacketBufferMut, RecvBatch, TxSlot, UdpSocket, UdpTransmit,
};

const PACKETS: u64 = 4096;
const BATCH: usize = 64;
const MAX_IN_FLIGHT: u64 = BATCH as u64;
const PAYLOAD_LEN: usize = 64;
const NO_PROGRESS_TIMEOUT: Duration = Duration::from_secs(1);

fn main() {
    let layout = BufferLayout::with_headroom_and_tailroom(256, 0, 0);
    let mut server = OsUdpSocketBuilder::new(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0).into())
        .buffer_layout(layout)
        .mtu(256)
        .bind()
        .unwrap();
    let server_addr = server.local_addr().unwrap();

    let mut client = OsUdpSocketBuilder::new(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0).into())
        .buffer_layout(layout)
        .mtu(256)
        .bind()
        .unwrap();

    let payload = make_payload();
    let mut rx = RecvBatch::with_capacity(BATCH);
    let mut tx = Vec::with_capacity(BATCH);

    let started = Instant::now();
    let mut last_progress = started;
    let mut sent: u64 = 0;
    let mut received: u64 = 0;

    while received < PACKETS {
        // Keep the receive queue below saturation. UDP send success only means
        // the datagram entered the kernel; if we flood loopback faster than we
        // drain it, the benchmark can lose packets and then wait forever.
        let in_flight = sent - received;
        if sent < PACKETS && in_flight < MAX_IN_FLIGHT {
            let batch_len = (PACKETS - sent)
                .min(MAX_IN_FLIGHT - in_flight)
                .min(BATCH as u64) as usize;
            build_batch(&mut tx, &mut client, &payload, server_addr, batch_len);
            match client.send(&mut tx) {
                Ok(0) => {}
                Ok(n) => {
                    sent += n as u64;
                    last_progress = Instant::now();
                }
                Err(error) => panic!("send failed: {error}"),
            }
        }

        rx.clear();
        match server.recv(&mut rx) {
            Ok(0) => {
                if last_progress.elapsed() > NO_PROGRESS_TIMEOUT {
                    panic!(
                        "direct_udp_one_way stalled after sending {sent} packets and receiving \
                         {received}; UDP datagrams were likely dropped"
                    );
                }
                thread::yield_now();
            }
            Ok(n) => {
                received += n as u64;
                last_progress = Instant::now();
                for item in rx.as_slice() {
                    black_box(item.packet.as_slice());
                }
            }
            Err(error) => panic!("recv failed: {error}"),
        }
    }

    let elapsed = started.elapsed();
    let ns_per_packet = elapsed.as_secs_f64() * 1e9 / PACKETS as f64;
    println!("direct_udp_one_way: {PACKETS} packets in {elapsed:?} ({ns_per_packet:.2} ns/packet)");
}

fn build_batch(
    slots: &mut Vec<TxSlot<UdpTransmit<OsPacketBuf>>>,
    socket: &mut OsUdpSocket,
    payload: &[u8; PAYLOAD_LEN],
    server_addr: std::net::SocketAddr,
    len: usize,
) {
    debug_assert!(len <= BATCH);
    slots.clear();
    slots.reserve(len);
    for _ in 0..len {
        let mut packet = socket
            .tx_pool_mut()
            .allocate()
            .expect("tx pool grows as needed");
        packet.extend_from_slice(payload).unwrap();
        slots.push(TxSlot::Ready(UdpTransmit::new(
            packet.freeze(),
            server_addr,
        )));
    }
}

fn make_payload() -> [u8; PAYLOAD_LEN] {
    let mut payload = [0u8; PAYLOAD_LEN];
    for (index, byte) in payload.iter_mut().enumerate() {
        *byte = index as u8;
    }
    payload
}
