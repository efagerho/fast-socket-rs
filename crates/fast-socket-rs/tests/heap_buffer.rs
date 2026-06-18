mod support;

use fast_socket_rs::{
    BufferAccessError, BufferLayout, PacketBuffer, PacketBufferMut, ReserveError, Segment,
    SegmentMut,
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
fn packet_buffer_exposes_borrowed_segments_and_contiguous_view() {
    let buffer = PacketBuf::copy_from_slice(b"hello");

    assert_eq!(buffer.first_segment(), Some(&b"hello"[..]));
    assert_eq!(buffer.contiguous(), Some(&b"hello"[..]));
}

#[test]
fn packet_buffer_mut_exposes_mutable_segments_and_contiguous_view() {
    let mut buffer = packet_buf_mut_from_slice(b"hello");

    buffer.first_segment_mut().unwrap()[0] = b'j';
    assert_eq!(buffer.as_slice(), b"jello");

    buffer.contiguous_mut().unwrap()[1] = b'a';
    assert_eq!(buffer.as_slice(), b"jallo");
}

struct SplitPacket {
    layout: BufferLayout,
    first: [u8; 3],
    second: [u8; 3],
}

impl SplitPacket {
    fn new() -> Self {
        Self {
            layout: BufferLayout::new(6).with_max_segments(2),
            first: *b"hel",
            second: *b"lo!",
        }
    }
}

impl PacketBuffer for SplitPacket {
    type Segments<'a> = std::array::IntoIter<Segment<'a>, 2>;

    fn len(&self) -> usize {
        6
    }

    fn headroom(&self) -> usize {
        0
    }

    fn tailroom(&self) -> usize {
        0
    }

    fn layout(&self) -> &BufferLayout {
        &self.layout
    }

    fn segments(&self) -> Self::Segments<'_> {
        [&self.first[..], &self.second[..]].into_iter()
    }

    fn read_at_exact(&self, offset: usize, dst: &mut [u8]) -> Result<(), BufferAccessError> {
        if offset
            .checked_add(dst.len())
            .is_none_or(|end| end > self.len())
        {
            return Err(BufferAccessError::OutOfBounds {
                offset,
                len: dst.len(),
                packet_len: self.len(),
            });
        }
        let mut packet = [0; 6];
        packet[..3].copy_from_slice(&self.first);
        packet[3..].copy_from_slice(&self.second);
        dst.copy_from_slice(&packet[offset..offset + dst.len()]);
        Ok(())
    }
}

impl PacketBufferMut for SplitPacket {
    type Frozen = Self;
    type SegmentsMut<'a> = std::array::IntoIter<SegmentMut<'a>, 2>;

    fn segments_mut(&mut self) -> Self::SegmentsMut<'_> {
        [&mut self.first[..], &mut self.second[..]].into_iter()
    }

    fn prepend(&mut self, bytes: &[u8]) -> Result<(), ReserveError> {
        Err(ReserveError::InsufficientHeadroom {
            available: 0,
            requested: bytes.len(),
        })
    }

    fn extend_from_slice(&mut self, bytes: &[u8]) -> Result<(), BufferAccessError> {
        Err(BufferAccessError::InsufficientTailroom {
            available: 0,
            requested: bytes.len(),
        })
    }

    fn trim_prefix(&mut self, len: usize) -> Result<(), BufferAccessError> {
        Err(BufferAccessError::OutOfBounds {
            offset: 0,
            len,
            packet_len: self.len(),
        })
    }

    fn trim_suffix(&mut self, len: usize) -> Result<(), BufferAccessError> {
        Err(BufferAccessError::OutOfBounds {
            offset: self.len().saturating_sub(len),
            len,
            packet_len: self.len(),
        })
    }

    fn freeze(self) -> Self::Frozen {
        self
    }
}

#[test]
fn split_packet_has_first_segment_but_no_contiguous_view() {
    let mut packet = SplitPacket::new();

    assert_eq!(packet.first_segment(), Some(&b"hel"[..]));
    assert_eq!(packet.contiguous(), None);

    {
        let first = packet.first_segment_mut().unwrap();
        assert_eq!(first, b"hel");
        first[0] = b'j';
    }

    assert_eq!(packet.first_segment(), Some(&b"jel"[..]));
    assert_eq!(packet.contiguous_mut(), None);
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
