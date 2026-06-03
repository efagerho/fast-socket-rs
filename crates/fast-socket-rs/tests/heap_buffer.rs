mod support;

use fast_socket_rs::{
    BufferAccessError, BufferLayout, BufferPool, PacketBuffer, PacketBufferMut, ReserveError,
};

use support::{HeapBufferPool, PacketBuf, PacketBufMut};

fn packet_buf_mut_from_slice(bytes: &[u8]) -> PacketBufMut {
    let mut buffer = PacketBufMut::new(BufferLayout::new(bytes.len()));
    buffer
        .extend_from_slice(bytes)
        .expect("fresh compact buffer has enough tailroom");
    buffer
}

#[test]
fn mutable_buffer_freezes_without_copying_semantics() {
    let layout = BufferLayout::with_headroom_and_tailroom(16, 4, 4);
    let mut buffer = PacketBufMut::new(layout);

    assert_eq!(buffer.headroom(), 4);
    assert_eq!(buffer.tailroom(), 20);

    buffer.extend_from_slice(b"hello").unwrap();
    assert_eq!(buffer.len(), 5);
    assert_eq!(buffer.as_slice(), b"hello");

    let frozen = buffer.freeze();
    assert_eq!(frozen.as_slice(), b"hello");
    assert_eq!(frozen.segments().collect::<Vec<_>>(), vec![&b"hello"[..]]);
}

#[test]
fn read_at_exact_rejects_short_reads() {
    let buffer = PacketBuf::copy_from_slice(b"abcdef");
    let mut bytes = [0; 3];

    buffer.read_at_exact(2, &mut bytes).unwrap();
    assert_eq!(&bytes, b"cde");

    assert!(matches!(
        buffer.read_at_exact(5, &mut bytes),
        Err(BufferAccessError::OutOfBounds { .. })
    ));
}

#[test]
fn prepend_writes_bytes_before_packet() {
    let layout = BufferLayout::with_headroom_and_tailroom(16, 8, 0);
    let mut buffer = PacketBufMut::new(layout);
    buffer.extend_from_slice(b"payload").unwrap();

    buffer.prepend(b"v4IP").unwrap();

    assert_eq!(buffer.as_slice(), b"v4IPpayload");
    assert_eq!(buffer.headroom(), 4);
}

#[test]
fn prepend_rejects_writes_exceeding_headroom() {
    let layout = BufferLayout::with_headroom_and_tailroom(16, 4, 0).with_l2_headroom(8);
    let mut buffer = PacketBufMut::new(layout);

    assert_eq!(buffer.headroom(), 4);
    assert!(matches!(
        buffer.prepend(&[0u8; 5]),
        Err(ReserveError::InsufficientHeadroom {
            available: 4,
            requested: 5,
        })
    ));
}

#[test]
fn relocating_prepend_and_extend_preserve_payload() {
    let mut prefix_buffer = packet_buf_mut_from_slice(b"payload");
    prefix_buffer.prepend_relocating(b"head").unwrap();
    assert_eq!(prefix_buffer.as_slice(), b"headpayload");

    let mut tail_buffer = packet_buf_mut_from_slice(b"payload");
    tail_buffer.extend_from_slice_relocating(b"tail").unwrap();
    assert_eq!(tail_buffer.as_slice(), b"payloadtail");
}

#[test]
fn heap_buffer_pool_allocates_contiguous_buffers() {
    let mut pool = HeapBufferPool::with_payload_capacity(16);
    assert_eq!(pool.layout().payload_capacity(), 16);

    let mut buffer = pool.allocate().unwrap();
    buffer.extend_from_slice(b"hello").unwrap();
    assert_eq!(buffer.as_slice(), b"hello");
}
