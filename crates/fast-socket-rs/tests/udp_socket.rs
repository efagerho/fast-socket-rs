mod support;

use std::net::{IpAddr, Ipv4Addr, SocketAddrV4};

use fast_socket_rs::{
    BusyPollDriver, Error, GenericUdpEndpoint, PollDriver, PollMode, RecvBatch, SendError,
    SocketId, TxSlot, UdpEndpointInfo, UdpEndpointSpec, UdpEndpointTransmit, UdpReceive,
    UdpRecvMeta, UdpSocket, UdpTransmit, UdpTxBufferMut, prepare_generic_udp_endpoint,
    send_generic_udp_endpoint,
};

use support::{HeapBufferPool, PacketBuf, PacketBufMut};

struct MockUdpSocket {
    rx_pool: HeapBufferPool,
    tx_pool: HeapBufferPool,
    driver: BusyPollDriver,
    sent: Vec<Vec<u8>>,
}

impl MockUdpSocket {
    fn new() -> Self {
        Self {
            rx_pool: HeapBufferPool::with_payload_capacity(128),
            tx_pool: HeapBufferPool::with_payload_capacity(128),
            driver: BusyPollDriver::new(),
            sent: Vec::new(),
        }
    }
}

impl UdpSocket for MockUdpSocket {
    type RxBuffer = PacketBufMut;
    type TxBufferMut = PacketBufMut;
    type Driver = BusyPollDriver;
    type RecvMeta = UdpRecvMeta;
    type Endpoint = GenericUdpEndpoint;

    fn socket_id(&self) -> SocketId {
        SocketId::new(7)
    }

    fn mtu(&self) -> usize {
        128
    }

    fn allocate_tx_batch(
        &mut self,
        out: &mut Vec<UdpTxBufferMut<Self>>,
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

    fn driver(&self) -> &Self::Driver {
        &self.driver
    }

    fn driver_mut(&mut self) -> &mut Self::Driver {
        &mut self.driver
    }

    fn send(&mut self, batch: &mut [TxSlot<UdpTransmit<PacketBuf>>]) -> Result<usize, SendError> {
        for (accepted, slot) in batch.iter_mut().enumerate() {
            let Some(tx) = slot.take() else {
                return Err(SendError {
                    accepted,
                    kind: Error::InvalidBatch,
                });
            };
            self.sent.push(tx.packet.as_slice().to_vec());
        }
        Ok(batch.len())
    }

    fn prepare_udp_endpoint(&mut self, spec: UdpEndpointSpec) -> Result<Self::Endpoint, Error> {
        prepare_generic_udp_endpoint(self, spec)
    }

    fn udp_endpoint_spec<'a>(&self, endpoint: &'a Self::Endpoint) -> &'a UdpEndpointSpec {
        endpoint.spec()
    }

    fn udp_endpoint_info(&self, endpoint: &Self::Endpoint) -> UdpEndpointInfo {
        endpoint.info()
    }

    fn send_to_udp_endpoint(
        &mut self,
        endpoint: &mut Self::Endpoint,
        batch: &mut [TxSlot<UdpEndpointTransmit<PacketBuf>>],
    ) -> Result<usize, SendError> {
        send_generic_udp_endpoint(self, endpoint, batch)
    }

    fn recv(
        &mut self,
        out: &mut RecvBatch<UdpReceive<PacketBufMut, Self::RecvMeta>>,
    ) -> Result<usize, Error> {
        let meta = UdpRecvMeta {
            source: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1234).into(),
            destination: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            destination_port: Some(5678),
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

#[test]
fn udp_endpoint_send_uses_prepared_metadata() {
    let mut socket = MockUdpSocket::new();
    let destination = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9999).into();
    let spec = UdpEndpointSpec {
        destination,
        payload_len: Some(4),
        ..UdpEndpointSpec::new(destination)
    };
    let mut endpoint = socket.prepare_udp_endpoint(spec.clone()).unwrap();

    assert_eq!(socket.udp_endpoint_spec(&endpoint), &spec);
    assert_eq!(
        socket.udp_endpoint_info(&endpoint),
        UdpEndpointInfo {
            mtu: 128,
            payload_len: Some(4),
            gso_segment_size: None,
        }
    );

    let packet = PacketBuf::copy_from_slice(b"ping");
    let mut tx = [TxSlot::Ready(UdpEndpointTransmit::new(packet))];
    assert_eq!(
        socket.send_to_udp_endpoint(&mut endpoint, &mut tx).unwrap(),
        1
    );
    assert!(tx[0].is_taken());
}

#[test]
fn udp_endpoint_batch_is_available_on_udp_socket_trait() {
    let mut socket = MockUdpSocket::new();
    let destination = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9999).into();
    let mut endpoint = socket
        .prepare_udp_endpoint(UdpEndpointSpec::new(destination))
        .unwrap();
    let payloads: [&[u8]; 3] = [b"first", b"second", b"third"];

    let accepted = socket
        .udp_endpoint_batch(&mut endpoint, payloads.len())
        .send(|index, payload| {
            let source = payloads[index];
            payload[..source.len()].copy_from_slice(source);
            source.len()
        })
        .unwrap();

    assert_eq!(accepted, payloads.len());
    assert_eq!(socket.sent, payloads.map(Vec::from));
}

#[test]
fn udp_endpoint_rejects_wrong_fixed_payload_len_without_consuming_slot() {
    let mut socket = MockUdpSocket::new();
    let destination = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9999).into();
    let mut endpoint = socket
        .prepare_udp_endpoint(UdpEndpointSpec {
            destination,
            payload_len: Some(4),
            ..UdpEndpointSpec::new(destination)
        })
        .unwrap();

    let packet = PacketBuf::copy_from_slice(b"too long");
    let mut tx = [TxSlot::Ready(UdpEndpointTransmit::new(packet))];
    let error = socket
        .send_to_udp_endpoint(&mut endpoint, &mut tx)
        .unwrap_err();

    assert_eq!(error.accepted, 0);
    assert!(matches!(error.kind, Error::InvalidPacket));
    assert!(tx[0].is_ready());
}
