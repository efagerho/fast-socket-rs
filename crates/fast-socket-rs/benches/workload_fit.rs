use std::collections::VecDeque;
use std::hint::black_box;
use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

use fast_socket_rs::{
    BufferLayout, BusyPollDriver, EgressResolver, Error, IpPacketReceive, IpPacketRecvMeta,
    IpPacketSocket, IpPacketTransmit, IpVersion, PacketBuffer, PacketBufferMut, RecvBatch,
    SendError, TxSlot, V4Only,
};

#[path = "../tests/support/mod.rs"]
mod support;

use support::{HeapBufferPool, PacketBuf, PacketBufMut};

const ITERATIONS: usize = 20_000;
const IPV4_HEADER_LEN: usize = 20;

#[derive(Debug)]
struct BenchIpPacketSocket {
    tx_pool: HeapBufferPool,
    driver: BusyPollDriver,
    recv: VecDeque<IpPacketReceive<PacketBufMut, IpPacketRecvMeta>>,
    sent_packets: usize,
    sent_bytes: usize,
}

impl BenchIpPacketSocket {
    fn new() -> Self {
        Self {
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
    type RxBuffer = PacketBufMut;
    type TxBufferMut = PacketBufMut;
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

    fn driver(&self) -> &Self::Driver {
        &self.driver
    }

    fn driver_mut(&mut self) -> &mut Self::Driver {
        &mut self.driver
    }

    fn allocate_tx_batch(
        &mut self,
        out: &mut Vec<fast_socket_rs::IpPacketTxBufferMut<Self>>,
        max: usize,
    ) -> Result<usize, Error> {
        let start_len = out.len();
        while out.len() - start_len < max {
            let Some(buffer) = self.tx_pool.allocate() else {
                break;
            };
            out.push(buffer);
        }
        Ok(out.len() - start_len)
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

fn main() {
    bench_direct_ip_packet_routing();
}
