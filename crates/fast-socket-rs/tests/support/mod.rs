#![allow(dead_code)]

use fast_socket_rs::{
    BufferAccessError, BufferLayout, BufferPool, OwnedPacketBuffer, PacketBuffer, PacketBufferMut,
    ReserveError, Segments,
};

#[derive(Debug)]
pub struct PacketBuf {
    storage: Vec<u8>,
    start: usize,
    end: usize,
    layout: BufferLayout,
}

impl PacketBuf {
    #[must_use]
    pub fn copy_from_slice(bytes: &[u8]) -> Self {
        let layout = BufferLayout::new(bytes.len());
        Self {
            storage: bytes.to_vec(),
            start: 0,
            end: bytes.len(),
            layout,
        }
    }

    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.storage[self.start..self.end]
    }
}

impl OwnedPacketBuffer for PacketBuf {
    type Mutable = PacketBufMut;

    fn into_mut(self) -> Self::Mutable {
        PacketBufMut {
            storage: self.storage,
            start: self.start,
            end: self.end,
            layout: self.layout,
        }
    }
}

impl PacketBuffer for PacketBuf {
    type Segments<'a> = Segments<'a>;

    fn len(&self) -> usize {
        self.end - self.start
    }

    fn headroom(&self) -> usize {
        self.start.saturating_sub(self.layout.l2_headroom())
    }

    fn tailroom(&self) -> usize {
        self.storage.len() - self.end
    }

    fn layout(&self) -> &BufferLayout {
        &self.layout
    }

    fn segments(&self) -> Self::Segments<'_> {
        if self.is_empty() {
            None.into_iter()
        } else {
            Some(self.as_slice()).into_iter()
        }
    }

    fn read_at_exact(&self, offset: usize, dst: &mut [u8]) -> Result<(), BufferAccessError> {
        read_contiguous(self.as_slice(), offset, dst)
    }
}

#[derive(Debug)]
pub struct PacketBufMut {
    storage: Vec<u8>,
    start: usize,
    end: usize,
    layout: BufferLayout,
}

impl PacketBufMut {
    #[must_use]
    pub fn new(layout: BufferLayout) -> Self {
        let storage = vec![0; layout.allocation_len()];
        let start = layout.data_offset();
        Self {
            storage,
            start,
            end: start,
            layout,
        }
    }

    #[must_use]
    pub fn copy_from_slice(bytes: &[u8]) -> Self {
        let mut buffer = Self::new(BufferLayout::new(bytes.len()));
        buffer
            .extend_from_slice(bytes)
            .expect("fresh compact buffer has enough tailroom");
        buffer
    }

    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.storage[self.start..self.end]
    }

    #[must_use]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.storage[self.start..self.end]
    }

    fn relocate(&mut self, headroom: usize, tailroom: usize) {
        let packet = self.as_slice().to_vec();
        let payload_capacity = self.layout.payload_capacity().max(packet.len());
        let layout = BufferLayout::with_headroom_and_tailroom(payload_capacity, headroom, tailroom)
            .with_l2_headroom(self.layout.l2_headroom())
            .with_alignment(self.layout.align())
            .with_max_segments(self.layout.max_segments());
        let mut storage = vec![0; layout.allocation_len()];
        let start = layout.data_offset();
        let end = start + packet.len();
        storage[start..end].copy_from_slice(&packet);

        self.storage = storage;
        self.start = start;
        self.end = end;
        self.layout = layout;
    }
}

impl PacketBuffer for PacketBufMut {
    type Segments<'a> = Segments<'a>;

    fn len(&self) -> usize {
        self.end - self.start
    }

    fn headroom(&self) -> usize {
        self.start.saturating_sub(self.layout.l2_headroom())
    }

    fn tailroom(&self) -> usize {
        self.storage.len() - self.end
    }

    fn layout(&self) -> &BufferLayout {
        &self.layout
    }

    fn segments(&self) -> Self::Segments<'_> {
        if self.is_empty() {
            None.into_iter()
        } else {
            Some(self.as_slice()).into_iter()
        }
    }

    fn read_at_exact(&self, offset: usize, dst: &mut [u8]) -> Result<(), BufferAccessError> {
        read_contiguous(self.as_slice(), offset, dst)
    }
}

impl PacketBufferMut for PacketBufMut {
    type Frozen = PacketBuf;

    fn prepend(&mut self, bytes: &[u8]) -> Result<(), ReserveError> {
        if bytes.len() > self.headroom() {
            return Err(ReserveError::InsufficientHeadroom {
                available: self.headroom(),
                requested: bytes.len(),
            });
        }
        let new_start = self.start - bytes.len();
        self.storage[new_start..self.start].copy_from_slice(bytes);
        self.start = new_start;
        Ok(())
    }

    fn prepend_relocating(&mut self, bytes: &[u8]) -> Result<(), ReserveError> {
        if bytes.len() > self.headroom() {
            self.relocate(self.headroom().max(bytes.len()), self.tailroom());
        }
        <Self as PacketBufferMut>::prepend(self, bytes)
    }

    fn extend_from_slice(&mut self, bytes: &[u8]) -> Result<(), BufferAccessError> {
        if bytes.len() > self.tailroom() {
            return Err(BufferAccessError::InsufficientTailroom {
                available: self.tailroom(),
                requested: bytes.len(),
            });
        }

        let next_end = self.end + bytes.len();
        self.storage[self.end..next_end].copy_from_slice(bytes);
        self.end = next_end;
        Ok(())
    }

    fn extend_from_slice_relocating(&mut self, bytes: &[u8]) -> Result<(), BufferAccessError> {
        if bytes.len() > self.tailroom() {
            self.relocate(self.headroom(), self.tailroom().max(bytes.len()));
        }
        <Self as PacketBufferMut>::extend_from_slice(self, bytes)
    }

    fn trim_prefix(&mut self, len: usize) -> Result<(), BufferAccessError> {
        if len > self.len() {
            return Err(BufferAccessError::OutOfBounds {
                offset: 0,
                len,
                packet_len: self.len(),
            });
        }
        self.start += len;
        Ok(())
    }

    fn trim_suffix(&mut self, len: usize) -> Result<(), BufferAccessError> {
        if len > self.len() {
            return Err(BufferAccessError::OutOfBounds {
                offset: self.len().saturating_sub(len),
                len,
                packet_len: self.len(),
            });
        }
        self.end -= len;
        Ok(())
    }

    fn freeze(self) -> Self::Frozen {
        PacketBuf {
            storage: self.storage,
            start: self.start,
            end: self.end,
            layout: self.layout,
        }
    }
}

#[derive(Debug)]
pub struct HeapBufferPool {
    layout: BufferLayout,
}

impl HeapBufferPool {
    #[must_use]
    pub fn new(layout: BufferLayout) -> Self {
        Self {
            layout: layout.with_max_segments(1),
        }
    }

    #[must_use]
    pub fn with_payload_capacity(payload_capacity: usize) -> Self {
        Self::new(BufferLayout::new(payload_capacity))
    }
}

impl BufferPool for HeapBufferPool {
    type Buffer = PacketBufMut;

    fn layout(&self) -> &BufferLayout {
        &self.layout
    }

    fn allocate(&mut self) -> Option<Self::Buffer> {
        Some(PacketBufMut::new(self.layout))
    }
}

fn read_contiguous(packet: &[u8], offset: usize, dst: &mut [u8]) -> Result<(), BufferAccessError> {
    let Some(end) = offset.checked_add(dst.len()) else {
        return Err(BufferAccessError::OutOfBounds {
            offset,
            len: dst.len(),
            packet_len: packet.len(),
        });
    };

    let Some(src) = packet.get(offset..end) else {
        return Err(BufferAccessError::OutOfBounds {
            offset,
            len: dst.len(),
            packet_len: packet.len(),
        });
    };

    dst.copy_from_slice(src);
    Ok(())
}
