use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::Duration;

use fast_socket_os_rs::OsUdpSocketBuilder;
use fast_socket_rs::{
    BufferLayout, BufferPool, PacketBufferMut, RecvBatch, TxSlot, UdpSocket, UdpTransmit,
};

#[test]
fn direct_udp_request_response_workload_fit() {
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

    let mut request = [TxSlot::Ready(UdpTransmit::new(
        tx_packet(&mut client, b"ping"),
        server_addr,
    ))];
    assert_eq!(client.send(&mut request).unwrap(), 1);

    let mut server_rx = RecvBatch::with_capacity(1);
    for _ in 0..50 {
        if server.recv(&mut server_rx).unwrap() == 1 {
            break;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(server_rx.len(), 1);
    assert_eq!(server_rx.as_slice()[0].packet.as_slice(), b"ping");
    let client_addr = server_rx.as_slice()[0].meta.source;

    let mut response = [TxSlot::Ready(UdpTransmit::new(
        tx_packet(&mut server, b"pong"),
        client_addr,
    ))];
    assert_eq!(server.send(&mut response).unwrap(), 1);

    let mut client_rx = RecvBatch::with_capacity(1);
    for _ in 0..50 {
        if client.recv(&mut client_rx).unwrap() == 1 {
            break;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(client_rx.len(), 1);
    assert_eq!(client_rx.as_slice()[0].packet.as_slice(), b"pong");
}

fn tx_packet(
    socket: &mut fast_socket_os_rs::OsUdpSocket,
    bytes: &[u8],
) -> fast_socket_os_rs::OsPacketBuf {
    let mut packet = socket.tx_pool_mut().allocate().unwrap();
    packet.extend_from_slice(bytes).unwrap();
    packet.freeze()
}
