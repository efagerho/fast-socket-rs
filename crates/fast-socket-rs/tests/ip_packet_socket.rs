mod support;

use fast_socket_rs::{
    BufferPool, BusyPollDriver, BusyPollIpPacketSocket, ChecksumStatus, CoreEgress, Error,
    IpPacketReceive, IpPacketRecvMeta, IpPacketSocket, IpPacketTransmit, IpVersion, NeighborId,
    RecvBatch, SendError, SocketId, TxOffload, TxSlot, V4Only,
};

use support::{HeapBufferPool, PacketBuf, PacketBufMut};

struct MockIpPacketSocket {
    rx_pool: HeapBufferPool,
    tx_pool: HeapBufferPool,
    driver: BusyPollDriver,
}

impl MockIpPacketSocket {
    fn new() -> Self {
        Self {
            rx_pool: HeapBufferPool::with_payload_capacity(1500),
            tx_pool: HeapBufferPool::with_payload_capacity(1500),
            driver: BusyPollDriver::new(),
        }
    }
}

impl IpPacketSocket for MockIpPacketSocket {
    type RxPool = HeapBufferPool;
    type TxPool = HeapBufferPool;
    type Family = V4Only;
    type Egress = CoreEgress;
    type Driver = BusyPollDriver;
    type RecvMeta = IpPacketRecvMeta;

    fn socket_id(&self) -> SocketId {
        SocketId::new(1)
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
        for (accepted, slot) in batch.iter_mut().enumerate() {
            if slot.take().is_none() {
                return Err(SendError {
                    accepted,
                    kind: Error::InvalidBatch,
                });
            }
        }
        Ok(batch.len())
    }

    fn recv(
        &mut self,
        out: &mut RecvBatch<IpPacketReceive<PacketBufMut, Self::RecvMeta>>,
    ) -> Result<usize, Error> {
        let packet = self.rx_pool.allocate().ok_or(Error::WouldBlock)?;
        out.push(IpPacketReceive::new(
            packet,
            IpPacketRecvMeta {
                version: IpVersion::V4,
                len: 0,
                checksum: ChecksumStatus::NotChecked,
            },
        ))
        .map_err(|_| Error::BatchFull)?;
        Ok(1)
    }

    fn drain_tx_completions(&mut self) -> Result<usize, Error> {
        Ok(0)
    }
}

fn assert_busy_poll_ip_packet_socket<S: BusyPollIpPacketSocket>(_socket: &S) {}

#[test]
fn ip_packet_socket_trait_surface_accepts_mock_socket() {
    let mut socket = MockIpPacketSocket::new();
    assert_busy_poll_ip_packet_socket(&socket);

    let packet = PacketBuf::copy_from_slice(&[0x45, 0, 0, 20]);
    let mut tx = [TxSlot::Ready(IpPacketTransmit::new(
        packet,
        CoreEgress::Neighbor(NeighborId::new(1)),
    ))];
    assert_eq!(socket.send(&mut tx).unwrap(), 1);
    assert!(tx[0].is_taken());

    let mut rx = RecvBatch::with_capacity(1);
    assert_eq!(socket.recv(&mut rx).unwrap(), 1);
    assert_eq!(rx.as_slice()[0].meta.version, IpVersion::V4);
}

#[test]
fn tx_offload_flags_compose_without_dependency() {
    let flags = TxOffload::CKSUM_IP | TxOffload::CKSUM_L4;
    assert!(flags.contains(TxOffload::CKSUM_IP));
    assert!(flags.contains(TxOffload::CKSUM_L4));
}
