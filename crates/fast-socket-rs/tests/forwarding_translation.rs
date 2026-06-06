mod support;

use fast_socket_rs::{BufferLayout, PacketBuffer, PacketBufferMut, ReserveError};

use support::PacketBufMut;

const IPV4_HEADER_LEN: usize = 20;
const IPV6_HEADER_LEN: usize = 40;

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
    let checksum = ipv4_checksum(&packet[..IPV4_HEADER_LEN]);
    packet[10..12].copy_from_slice(&checksum.to_be_bytes());
    packet
}

fn ipv4_checksum(header: &[u8]) -> u16 {
    let mut sum = 0u32;
    for chunk in header.chunks_exact(2) {
        sum += u32::from(u16::from_be_bytes([chunk[0], chunk[1]]));
    }
    while (sum >> 16) != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

fn decrement_ipv4_ttl(packet: &mut PacketBufMut) {
    let bytes = packet.as_mut_slice();
    let header_len = usize::from(bytes[0] & 0x0f) * 4;
    assert!(bytes[8] > 1, "test packet must be forwardable");
    bytes[8] -= 1;
    bytes[10..12].copy_from_slice(&0u16.to_be_bytes());
    let checksum = ipv4_checksum(&bytes[..header_len]);
    bytes[10..12].copy_from_slice(&checksum.to_be_bytes());
}

fn write_ipv6_header(header: &mut [u8; IPV6_HEADER_LEN], payload_len: usize) {
    header[0] = 0x60;
    header[4..6].copy_from_slice(&(payload_len as u16).to_be_bytes());
    header[6] = 17;
    header[7] = 63;
    header[8..24].copy_from_slice(&[0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    header[24..40].copy_from_slice(&[0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]);
}

#[test]
fn ip_packet_forwarding_updates_ttl_in_place_and_preserves_payload() {
    let payload = b"forwarded payload";
    let mut packet = PacketBufMut::copy_from_slice(&ipv4_packet(payload, 64));
    let before_len = packet.len();

    decrement_ipv4_ttl(&mut packet);

    assert_eq!(packet.len(), before_len);
    assert_eq!(packet.as_slice()[8], 63);
    assert_eq!(&packet.as_slice()[IPV4_HEADER_LEN..], payload);
    assert_eq!(ipv4_checksum(&packet.as_slice()[..IPV4_HEADER_LEN]), 0);
}

#[test]
fn ipv4_to_ipv6_translation_uses_prepend_and_preserves_payload() {
    let payload = b"translated payload";
    let mut packet = PacketBufMut::new(BufferLayout::with_headroom_and_tailroom(
        payload.len(),
        IPV6_HEADER_LEN,
        0,
    ));
    packet.extend_from_slice(payload).unwrap();

    let mut header = [0u8; IPV6_HEADER_LEN];
    write_ipv6_header(&mut header, payload.len());
    let starting_headroom = packet.headroom();
    packet.prepend(&header).unwrap();

    assert_eq!(starting_headroom, IPV6_HEADER_LEN);
    assert_eq!(packet.headroom(), 0);
    assert_eq!(packet.len(), IPV6_HEADER_LEN + payload.len());
    assert_eq!(&packet.as_slice()[..IPV6_HEADER_LEN], &header);
    assert_eq!(&packet.as_slice()[IPV6_HEADER_LEN..], payload);
}

#[test]
fn extend_appends_translation_suffix_and_trim_returns_tailroom() {
    let payload = b"inner datagram";
    let suffix = [0xde, 0xad, 0xbe, 0xef];
    let mut packet = PacketBufMut::new(BufferLayout::with_headroom_and_tailroom(
        payload.len(),
        0,
        suffix.len(),
    ));
    packet.extend_from_slice(payload).unwrap();
    packet.extend_from_slice(&suffix).unwrap();

    assert_eq!(packet.as_slice(), b"inner datagram\xde\xad\xbe\xef");
    packet.trim_suffix(suffix.len()).unwrap();
    assert_eq!(packet.as_slice(), payload);
    assert_eq!(packet.tailroom(), suffix.len());
}

#[test]
fn headroom_and_tailroom_exhaustion_are_explicit_fast_path_errors() {
    let mut packet = PacketBufMut::new(BufferLayout::with_headroom_and_tailroom(16, 4, 2));
    packet.extend_from_slice(b"0123456789abcdef").unwrap();

    assert_eq!(
        packet.prepend(&[0u8; 5]).unwrap_err(),
        ReserveError::InsufficientHeadroom {
            available: 4,
            requested: 5,
        }
    );
}
