use std::hint::black_box;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::{Duration, Instant};

use fast_socket_os_rs::{OsPacketBufMut, OsUdpSocket, OsUdpSocketBuilder};
use fast_socket_rs::{
    BufferLayout, BufferPool, PacketBufferMut, RecvBatch, TxSlot, UdpReceive, UdpRecvMeta,
    UdpSocket, UdpTransmit,
};

const ITERATIONS: usize = 512;

fn recv_one(
    socket: &mut OsUdpSocket,
    out: &mut RecvBatch<UdpReceive<OsPacketBufMut, UdpRecvMeta>>,
) {
    out.clear();
    for _ in 0..100 {
        if socket.recv(out).unwrap() == 1 {
            return;
        }
        std::thread::sleep(Duration::from_micros(50));
    }
    panic!("timed out waiting for UDP packet");
}

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

    let mut server_rx = RecvBatch::with_capacity(1);
    let mut client_rx = RecvBatch::with_capacity(1);
    let started = Instant::now();

    for _ in 0..ITERATIONS {
        let mut request = [TxSlot::Ready(UdpTransmit::new(
            tx_packet(&mut client, black_box(b"ping")),
            server_addr,
        ))];
        assert_eq!(client.send(&mut request).unwrap(), 1);

        recv_one(&mut server, &mut server_rx);
        let client_addr = server_rx.as_slice()[0].meta.source;
        black_box(server_rx.as_slice()[0].packet.as_slice());

        let mut response = [TxSlot::Ready(UdpTransmit::new(
            tx_packet(&mut server, black_box(b"pong")),
            client_addr,
        ))];
        assert_eq!(server.send(&mut response).unwrap(), 1);

        recv_one(&mut client, &mut client_rx);
        black_box(client_rx.as_slice()[0].packet.as_slice());
    }

    let elapsed = started.elapsed();
    let ns_per_round_trip = elapsed.as_nanos() as f64 / ITERATIONS as f64;
    println!(
        "direct_udp_request_response: {ITERATIONS} round trips, {elapsed:?}, {ns_per_round_trip:.2} ns/round-trip"
    );
}

fn tx_packet(socket: &mut OsUdpSocket, bytes: &[u8]) -> fast_socket_os_rs::OsPacketBuf {
    let mut packet = socket.tx_pool_mut().allocate().unwrap();
    packet.extend_from_slice(bytes).unwrap();
    packet.freeze()
}
