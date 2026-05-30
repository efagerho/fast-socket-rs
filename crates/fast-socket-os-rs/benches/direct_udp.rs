//! Bench: many UDP datagrams in flight at once, measuring per-packet cost
//! through the `OsUdpSocket` send + recv path. The previous version did one
//! round trip at a time with `thread::sleep(50µs)` between calls, which
//! measured the sleep granularity (~1ms kernel HZ on most boxes) rather than
//! the socket path. The current version drains as many datagrams as possible
//! per syscall and walks the loop until every sent packet has been received,
//! with `yield_now`-only back-pressure when the socket reports no progress.

use std::hint::black_box;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::thread;
use std::time::Instant;

use fast_socket_os_rs::{OsPacketBuf, OsUdpSocket, OsUdpSocketBuilder};
use fast_socket_rs::{
    BufferLayout, BufferPool, PacketBufferMut, RecvBatch, TxSlot, UdpSocket, UdpTransmit,
};

const PACKETS: u64 = 4096;
const BATCH: usize = 64;
const PAYLOAD_LEN: usize = 64;

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

    let started = Instant::now();
    let mut sent: u64 = 0;
    let mut received: u64 = 0;

    while received < PACKETS {
        // Push another batch of up to BATCH packets while there is still room
        // in the OS send buffer and we have not yet sent PACKETS in total.
        while sent < PACKETS {
            let mut batch = build_batch(&mut client, &payload, server_addr, sent);
            match client.send(&mut batch) {
                Ok(0) => break, // send buffer full this iteration
                Ok(n) => sent += n as u64,
                Err(error) => panic!("send failed: {error}"),
            }
            // If we sent fewer than the batch, give the receive path a turn.
            if batch.iter().any(|slot| !slot.is_taken()) {
                break;
            }
        }

        rx.clear();
        match server.recv(&mut rx) {
            Ok(0) => thread::yield_now(),
            Ok(n) => {
                received += n as u64;
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
    socket: &mut OsUdpSocket,
    payload: &[u8; PAYLOAD_LEN],
    server_addr: std::net::SocketAddr,
    sent: u64,
) -> [TxSlot<UdpTransmit<OsPacketBuf>>; BATCH] {
    // We need to construct an array of size BATCH. Build a Vec then convert.
    let _ = sent;
    let mut slots: Vec<TxSlot<UdpTransmit<OsPacketBuf>>> = Vec::with_capacity(BATCH);
    for _ in 0..BATCH {
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
    slots
        .try_into()
        .map_err(|_| ())
        .expect("BATCH-sized Vec converts to BATCH-sized array")
}

fn make_payload() -> [u8; PAYLOAD_LEN] {
    let mut payload = [0u8; PAYLOAD_LEN];
    for (index, byte) in payload.iter_mut().enumerate() {
        *byte = index as u8;
    }
    payload
}
