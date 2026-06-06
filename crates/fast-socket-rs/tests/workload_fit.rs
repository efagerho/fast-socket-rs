use std::collections::VecDeque;
use std::net::Ipv4Addr;

use fast_socket_rs::{
    BufferLayout, BusyPollDriver, EgressResolver, Error, IpPacketReceive, IpPacketRecvMeta,
    IpPacketSocket, IpPacketTransmit, IpVersion, PacketBufferMut, RecvBatch, SendError, TxSlot,
    V4Only,
};

mod support;

use support::{HeapBufferPool, PacketBuf, PacketBufMut};

const IPV4_HEADER_LEN: usize = 20;

#[derive(Debug)]
struct CaptureIpPacketSocket {
    rx_pool: HeapBufferPool,
    tx_pool: HeapBufferPool,
    driver: BusyPollDriver,
    recv: VecDeque<IpPacketReceive<PacketBufMut, IpPacketRecvMeta>>,
    sent: Vec<IpPacketTransmit<PacketBuf, (), V4Only>>,
}

impl CaptureIpPacketSocket {
    fn new() -> Self {
        Self {
            rx_pool: HeapBufferPool::new(BufferLayout::with_headroom_and_tailroom(2048, 64, 64)),
            tx_pool: HeapBufferPool::new(BufferLayout::with_headroom_and_tailroom(2048, 64, 64)),
            driver: BusyPollDriver::new(),
            recv: VecDeque::new(),
            sent: Vec::new(),
        }
    }

    fn push_ipv4(&mut self, packet: PacketBufMut) {
        self.recv.push_back(IpPacketReceive::new(
            packet,
            IpPacketRecvMeta {
                version: IpVersion::V4,
                len: 0,
                checksum: fast_socket_rs::ChecksumStatus::NotChecked,
            },
        ));
    }
}

impl IpPacketSocket for CaptureIpPacketSocket {
    type RxPool = HeapBufferPool;
    type TxPool = HeapBufferPool;
    type Family = V4Only;
    type Egress = ();
    type Driver = BusyPollDriver;
    type RecvMeta = IpPacketRecvMeta;

    fn socket_id(&self) -> fast_socket_rs::SocketId {
        fast_socket_rs::SocketId::new(0)
    }

    fn mtu(&self) -> usize {
        1500
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

    fn send(
        &mut self,
        batch: &mut [TxSlot<IpPacketTransmit<PacketBuf, Self::Egress, Self::Family>>],
    ) -> Result<usize, SendError> {
        let mut accepted = 0;
        for slot in batch.iter_mut() {
            let Some(tx) = slot.take() else {
                return Err(SendError {
                    accepted,
                    kind: Error::InvalidBatch,
                });
            };
            self.sent.push(tx);
            accepted += 1;
        }
        Ok(accepted)
    }

    fn recv(
        &mut self,
        out: &mut RecvBatch<IpPacketReceive<PacketBufMut, Self::RecvMeta>>,
    ) -> Result<usize, Error> {
        let mut delivered = 0;
        while out.remaining() > 0 {
            let Some(packet) = self.recv.pop_front() else {
                break;
            };
            out.push(packet).map_err(|_| Error::BatchFull)?;
            delivered += 1;
        }
        Ok(delivered)
    }

    fn drain_tx_completions(&mut self) -> Result<usize, Error> {
        Ok(0)
    }
}

#[derive(Clone, Copy, Debug)]
struct StaticResolver;

impl EgressResolver<V4Only, ()> for StaticResolver {
    fn resolve_egress(&self, _dst: Ipv4Addr) -> Option<()> {
        Some(())
    }
}

fn ipv4_packet(payload: &[u8], ttl: u8) -> Vec<u8> {
    let total_len = IPV4_HEADER_LEN + payload.len();
    let mut packet = vec![0u8; total_len];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
    packet[8] = ttl;
    packet[9] = 17;
    packet[12..16].copy_from_slice(&[192, 0, 2, 1]);
    packet[16..20].copy_from_slice(&[198, 51, 100, 1]);
    packet[IPV4_HEADER_LEN..].copy_from_slice(payload);
    packet
}

fn route_one<R>(
    socket: &mut CaptureIpPacketSocket,
    resolver: &R,
    dst: Ipv4Addr,
) -> Result<usize, Error>
where
    R: EgressResolver<V4Only, ()>,
{
    let mut rx = RecvBatch::with_capacity(1);
    if socket.recv(&mut rx)? == 0 {
        return Ok(0);
    }
    let mut item = rx.drain().next().expect("one received item");
    item.packet.as_mut_slice()[8] -= 1;
    resolver.resolve_egress(dst).ok_or(Error::NoEgressRoute)?;
    let mut tx = [TxSlot::Ready(IpPacketTransmit::new(
        item.packet.freeze(),
        (),
    ))];
    socket
        .send(&mut tx)
        .map_err(|error| error.kind)
        .map(|accepted| accepted.min(1))
}

#[test]
fn direct_ip_packet_routing_workload_fits_core_traits() {
    let mut ip_socket = CaptureIpPacketSocket::new();
    ip_socket.push_ipv4(PacketBufMut::copy_from_slice(&ipv4_packet(b"route me", 64)));

    assert_eq!(
        route_one(
            &mut ip_socket,
            &StaticResolver,
            Ipv4Addr::new(198, 51, 100, 1),
        )
        .unwrap(),
        1
    );
    assert_eq!(ip_socket.sent.len(), 1);
    assert_eq!(ip_socket.sent[0].packet.as_slice()[8], 63);
    assert_eq!(
        &ip_socket.sent[0].packet.as_slice()[IPV4_HEADER_LEN..],
        b"route me"
    );
}
