mod support;

use std::net::{IpAddr, Ipv4Addr, SocketAddrV4};

use fast_socket_rs::{
    BufferPool, BusyPollDriver, Error, PollDriver, PollMode, RecvBatch, SendError, SocketId,
    TxSlot, UdpReceive, UdpRecvMeta, UdpSocket, UdpTransmit,
};

use support::{HeapBufferPool, PacketBuf, PacketBufMut};

struct MockUdpSocket {
    rx_pool: HeapBufferPool,
    tx_pool: HeapBufferPool,
    driver: BusyPollDriver,
}

impl MockUdpSocket {
    fn new() -> Self {
        Self {
            rx_pool: HeapBufferPool::with_payload_capacity(128),
            tx_pool: HeapBufferPool::with_payload_capacity(128),
            driver: BusyPollDriver::new(),
        }
    }
}

impl UdpSocket for MockUdpSocket {
    type RxPool = HeapBufferPool;
    type TxPool = HeapBufferPool;
    type Driver = BusyPollDriver;
    type RecvMeta = UdpRecvMeta;

    fn socket_id(&self) -> SocketId {
        SocketId::new(7)
    }

    fn mtu(&self) -> usize {
        128
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

    fn send(&mut self, batch: &mut [TxSlot<UdpTransmit<PacketBuf>>]) -> Result<usize, SendError> {
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
        out: &mut RecvBatch<UdpReceive<PacketBufMut, Self::RecvMeta>>,
    ) -> Result<usize, Error> {
        let meta = UdpRecvMeta {
            source: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1234).into(),
            destination: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            ecn: None,
            len: 0,
            gro_stride: None,
        };
        let packet = self.rx_pool.allocate().ok_or(Error::WouldBlock)?;
        out.push(UdpReceive::new(packet, meta))
            .map_err(|_| Error::BatchFull)?;
        Ok(1)
    }

    fn drain_tx_completions(&mut self) -> Result<usize, Error> {
        Ok(0)
    }
}

#[test]
fn udp_socket_trait_surface_accepts_mock_socket() {
    let mut socket = MockUdpSocket::new();
    assert_eq!(
        <<MockUdpSocket as UdpSocket>::Driver as PollDriver>::MODE,
        PollMode::BusyPoll
    );
    assert_eq!(socket.socket_id(), SocketId::new(7));

    let destination = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9999).into();
    let packet = PacketBuf::copy_from_slice(b"ping");
    let mut tx = [TxSlot::Ready(UdpTransmit::new(packet, destination))];
    assert_eq!(socket.send(&mut tx).unwrap(), 1);
    assert!(tx[0].is_taken());

    let mut rx = RecvBatch::with_capacity(4);
    assert_eq!(socket.recv(&mut rx).unwrap(), 1);
    assert_eq!(rx.len(), 1);
}
