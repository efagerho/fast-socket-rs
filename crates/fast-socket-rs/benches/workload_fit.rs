use std::collections::VecDeque;
use std::hint::black_box;
use std::marker::PhantomData;
use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

use fast_socket_rs::{
    BufferLayout, BusyPollDriver, EgressResolver, Error, HeapBufferPool, IpPacketReceive,
    IpPacketRecvMeta, IpPacketSocket, IpPacketTransmit, IpVersion, PacketBuf, PacketBufMut,
    PacketBuffer, PacketBufferMut, RecvBatch, SendError, TxSlot, V4Only,
};

const ITERATIONS: usize = 20_000;
const IPV4_HEADER_LEN: usize = 20;

#[derive(Debug)]
struct BenchIpPacketSocket {
    rx_pool: HeapBufferPool,
    tx_pool: HeapBufferPool,
    driver: BusyPollDriver,
    recv: VecDeque<IpPacketReceive<PacketBufMut, IpPacketRecvMeta>>,
    sent_packets: usize,
    sent_bytes: usize,
}

impl BenchIpPacketSocket {
    fn new() -> Self {
        Self {
            rx_pool: HeapBufferPool::new(BufferLayout::with_headroom_and_tailroom(2048, 64, 64)),
            tx_pool: HeapBufferPool::new(BufferLayout::with_headroom_and_tailroom(2048, 64, 64)),
            driver: BusyPollDriver::new(),
            recv: VecDeque::new(),
            sent_packets: 0,
            sent_bytes: 0,
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

impl IpPacketSocket for BenchIpPacketSocket {
    type RxPool = HeapBufferPool;
    type TxPool = HeapBufferPool;
    type Family = V4Only;
    type Egress = ();
    type Driver = BusyPollDriver;
    type RecvMeta = IpPacketRecvMeta;

    fn queue_id(&self) -> fast_socket_rs::QueueId {
        fast_socket_rs::QueueId::new(0)
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
            self.sent_packets += 1;
            self.sent_bytes += tx.packet.len();
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

trait TunnelPolicy {
    const HEADER_LEN: usize;

    fn write_header(header: &mut [u8], inner_len: usize);
}

#[derive(Clone, Copy, Debug)]
struct Ipv4InIpv4;

impl TunnelPolicy for Ipv4InIpv4 {
    const HEADER_LEN: usize = IPV4_HEADER_LEN;

    fn write_header(header: &mut [u8], inner_len: usize) {
        header[0] = 0x45;
        header[2..4].copy_from_slice(&((Self::HEADER_LEN + inner_len) as u16).to_be_bytes());
        header[8] = 64;
        header[9] = 4;
        header[12..16].copy_from_slice(&[203, 0, 113, 1]);
        header[16..20].copy_from_slice(&[203, 0, 113, 2]);
    }
}

#[derive(Clone, Copy, Debug)]
struct TypedTunnel<P> {
    _policy: PhantomData<P>,
}

impl<P> TypedTunnel<P>
where
    P: TunnelPolicy,
{
    fn encapsulate(packet: &mut PacketBufMut) {
        let mut header = vec![0u8; P::HEADER_LEN];
        P::write_header(&mut header, packet.len());
        packet.prepend(&header).unwrap();
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

fn route_one(socket: &mut BenchIpPacketSocket, resolver: &StaticResolver) {
    let mut rx = RecvBatch::with_capacity(1);
    if socket.recv(&mut rx).unwrap() == 0 {
        return;
    }
    let mut item = rx.drain().next().unwrap();
    item.packet.as_mut_slice()[8] -= 1;
    resolver
        .resolve_egress(Ipv4Addr::new(198, 51, 100, 1))
        .unwrap();
    let mut tx = [TxSlot::Ready(IpPacketTransmit::new(
        item.packet.freeze(),
        (),
    ))];
    let accepted = socket.send(&mut tx).unwrap();
    black_box(accepted);
}

fn run(name: &str, iterations: usize, mut f: impl FnMut()) -> Duration {
    let started = Instant::now();
    for _ in 0..iterations {
        f();
    }
    let elapsed = started.elapsed();
    let ns_per_iter = elapsed.as_secs_f64() * 1e9 / iterations as f64;
    println!("{name}: {iterations} iterations, {elapsed:?}, {ns_per_iter:.2} ns/iter");
    elapsed
}

fn bench_direct_ip_packet_routing() {
    let resolver = StaticResolver;
    let packet = ipv4_packet(b"route benchmark payload", 64);
    let mut socket = BenchIpPacketSocket::new();

    run("direct_ip_packet_routing", ITERATIONS, || {
        socket.push_ipv4(PacketBufMut::copy_from_slice(black_box(&packet)));
        route_one(&mut socket, &resolver);
    });
    black_box(socket.sent_packets);
}

fn bench_typed_tunnel() {
    let inner = ipv4_packet(b"inner benchmark payload", 32);

    run("typed_ip_in_ip_tunnel", ITERATIONS, || {
        let mut packet = PacketBufMut::new(BufferLayout::with_headroom_and_tailroom(
            inner.len(),
            Ipv4InIpv4::HEADER_LEN,
            0,
        ));
        packet.extend_from_slice(black_box(&inner)).unwrap();
        TypedTunnel::<Ipv4InIpv4>::encapsulate(&mut packet);
        black_box(packet.len());
    });
}

fn main() {
    bench_direct_ip_packet_routing();
    bench_typed_tunnel();
}
