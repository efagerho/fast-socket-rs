//! Shared packet buffer traits and a small heap-backed implementation.

use core::fmt;
use core::num::NonZeroUsize;

/// A borrowed packet segment.
pub type Segment<'a> = &'a [u8];

/// Iterator over packet segments in packet-byte order.
///
/// The heap-backed buffers in this crate always have at most one segment, so
/// `Segments` is an alias for `Option::into_iter()`. Multi-segment backends
/// (AF_XDP UMEM, DPDK mempools) define their own iterator type.
pub type Segments<'a> = core::option::IntoIter<Segment<'a>>;

/// Borrowed scatter-gather view over packet segments.
#[derive(Clone, Copy, Debug)]
pub struct ScatterGather<'a> {
    segments: &'a [Segment<'a>],
}

impl<'a> ScatterGather<'a> {
    /// Creates a scatter-gather view from a borrowed segment slice.
    #[must_use]
    pub const fn new(segments: &'a [Segment<'a>]) -> Self {
        Self { segments }
    }

    /// Returns the borrowed segments.
    #[must_use]
    pub const fn segments(self) -> &'a [Segment<'a>] {
        self.segments
    }
}

/// Buffer layout facts shared by pools, queues, and backends.
///
/// Field invariants:
///
/// - `data_offset == l2_headroom + headroom`
/// - `chunk_size >= data_offset + payload_capacity + tailroom`
/// - `stride >= chunk_size`
///
/// `align` and `max_segments` are descriptive facts that backends with their
/// own allocators must honor. The heap-backed [`HeapBufferPool`] does not
/// enforce them; it always produces single-segment, default-aligned buffers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BufferLayout {
    payload_capacity: usize,
    headroom: usize,
    tailroom: usize,
    chunk_size: usize,
    data_offset: usize,
    align: NonZeroUsize,
    stride: usize,
    max_segments: usize,
    l2_headroom: usize,
    chunk_fixed: bool,
}

impl BufferLayout {
    /// Creates a contiguous packet layout with no explicit headroom or tailroom.
    #[must_use]
    pub fn new(payload_capacity: usize) -> Self {
        Self::with_headroom_and_tailroom(payload_capacity, 0, 0)
    }

    /// Creates a layout sized for a UDP/IP-like payload of at most
    /// `payload_capacity` bytes, rounded up to at least 2 KiB so a single
    /// buffer comfortably holds an MTU-sized Ethernet frame after backend-
    /// reserved L2 headroom is added.
    ///
    /// Equivalent to `BufferLayout::new(payload_capacity.max(2048))`. Use
    /// this instead of repeating the magic `2048` constant at every callsite.
    #[must_use]
    pub fn for_payload(payload_capacity: usize) -> Self {
        const PACKET_CAPACITY_FLOOR: usize = 2048;
        Self::new(payload_capacity.max(PACKET_CAPACITY_FLOOR))
    }

    /// Creates a contiguous packet layout with public headroom and tailroom.
    #[must_use]
    pub fn with_headroom_and_tailroom(
        payload_capacity: usize,
        headroom: usize,
        tailroom: usize,
    ) -> Self {
        let align = NonZeroUsize::new(1).expect("1 is non-zero");
        let data_offset = headroom;
        let chunk_size = data_offset + payload_capacity + tailroom;
        Self {
            payload_capacity,
            headroom,
            tailroom,
            chunk_size,
            data_offset,
            align,
            stride: chunk_size,
            max_segments: 1,
            l2_headroom: 0,
            chunk_fixed: false,
        }
    }

    /// Returns a copy of this layout with backend-reserved L2 headroom.
    ///
    /// When the chunk has not been fixed via [`Self::with_fixed_chunk`], the
    /// chunk size and stride grow to accommodate the new data offset. When the
    /// chunk has been fixed, the existing chunk is preserved and must still
    /// satisfy the layout invariants; a violation panics rather than silently
    /// corrupting the layout.
    #[must_use]
    pub fn with_l2_headroom(mut self, l2_headroom: usize) -> Self {
        self.l2_headroom = l2_headroom;
        self.data_offset = self.l2_headroom + self.headroom;
        // The packet's first byte sits at `data_offset` bytes into each chunk,
        // so it must still satisfy the layout's alignment requirement no matter
        // which branch we take below.
        let align = self.align.get();
        assert!(
            self.data_offset.is_multiple_of(align),
            "with_l2_headroom violates alignment: data_offset {} not a multiple of {align}",
            self.data_offset,
        );
        if !self.chunk_fixed {
            self.chunk_size = self.data_offset + self.payload_capacity + self.tailroom;
            self.stride = self.chunk_size;
            return self;
        }

        let minimum = self.minimum_chunk_size();
        assert!(
            self.chunk_size >= minimum,
            "with_l2_headroom invalidates fixed chunk: chunk_size {} < minimum {minimum}",
            self.chunk_size,
        );
        self
    }

    /// Returns a copy of this layout with a required data alignment.
    ///
    /// Panics if `align` is not a power of two.
    #[must_use]
    pub const fn with_alignment(mut self, align: NonZeroUsize) -> Self {
        assert!(
            align.get().is_power_of_two(),
            "BufferLayout alignment must be a power of two",
        );
        self.align = align;
        self
    }

    /// Returns a copy of this layout with a maximum segment count.
    ///
    /// Panics if `max_segments` is zero. A layout with zero segments cannot
    /// describe any packet.
    #[must_use]
    pub const fn with_max_segments(mut self, max_segments: usize) -> Self {
        assert!(
            max_segments >= 1,
            "BufferLayout max_segments must be at least 1",
        );
        self.max_segments = max_segments;
        self
    }

    /// Returns a copy of this layout with fixed chunk and stride facts.
    ///
    /// This is useful for describing AF_XDP UMEM frames, DPDK-style mbuf
    /// storage, and other fixed-size packet memory. `chunk_size` must fit the
    /// configured L2 headroom, public headroom, payload capacity, and tailroom.
    /// `stride` must be at least as large as the chunk size.
    pub fn with_fixed_chunk(
        mut self,
        chunk_size: usize,
        stride: usize,
    ) -> Result<Self, BufferLayoutError> {
        let minimum = self.minimum_chunk_size();
        if chunk_size < minimum {
            return Err(BufferLayoutError::ChunkTooSmall {
                minimum,
                requested: chunk_size,
            });
        }

        if stride < chunk_size {
            return Err(BufferLayoutError::StrideTooSmall {
                chunk_size,
                requested: stride,
            });
        }

        self.chunk_size = chunk_size;
        self.stride = stride;
        self.chunk_fixed = true;
        Ok(self)
    }

    /// Usable packet payload/datagram capacity.
    #[must_use]
    pub const fn payload_capacity(self) -> usize {
        self.payload_capacity
    }

    /// Public prefix space before the initial packet start.
    #[must_use]
    pub const fn headroom(self) -> usize {
        self.headroom
    }

    /// Public suffix space after the initial packet end.
    #[must_use]
    pub const fn tailroom(self) -> usize {
        self.tailroom
    }

    /// Backing chunk size for fixed-size packet memory.
    #[must_use]
    pub const fn chunk_size(self) -> usize {
        self.chunk_size
    }

    /// Initial offset from backing memory to packet bytes.
    #[must_use]
    pub const fn data_offset(self) -> usize {
        self.data_offset
    }

    /// Required data alignment.
    #[must_use]
    pub const fn align(self) -> NonZeroUsize {
        self.align
    }

    /// Distance between fixed-size chunks.
    #[must_use]
    pub const fn stride(self) -> usize {
        self.stride
    }

    /// Maximum supported scatter-gather segment count.
    #[must_use]
    pub const fn max_segments(self) -> usize {
        self.max_segments
    }

    /// Backend-reserved link-layer headroom.
    #[must_use]
    pub const fn l2_headroom(self) -> usize {
        self.l2_headroom
    }

    /// Total allocation length needed by the heap-backed implementation.
    #[must_use]
    pub const fn allocation_len(self) -> usize {
        self.chunk_size
    }

    const fn minimum_chunk_size(self) -> usize {
        self.data_offset + self.payload_capacity + self.tailroom
    }
}

/// Error returned when constructing an invalid [`BufferLayout`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum BufferLayoutError {
    /// The requested chunk size cannot hold the configured packet layout.
    ChunkTooSmall {
        /// Minimum required chunk size.
        minimum: usize,
        /// Requested chunk size.
        requested: usize,
    },
    /// The requested stride is smaller than the fixed chunk size.
    StrideTooSmall {
        /// Fixed chunk size.
        chunk_size: usize,
        /// Requested stride.
        requested: usize,
    },
}

impl fmt::Display for BufferLayoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ChunkTooSmall { minimum, requested } => write!(
                f,
                "buffer chunk too small: requested {requested}, minimum {minimum}"
            ),
            Self::StrideTooSmall {
                chunk_size,
                requested,
            } => write!(
                f,
                "buffer stride too small: requested {requested}, chunk size {chunk_size}"
            ),
        }
    }
}

impl std::error::Error for BufferLayoutError {}

/// Per-queue buffer layout and descriptor-depth configuration.
#[derive(Clone, Copy, Debug)]
pub struct QueueBufferConfig {
    /// Receive buffer layout.
    pub rx: BufferLayout,
    /// Transmit buffer layout.
    pub tx: BufferLayout,
    /// Receive descriptor depth when exposed by a backend.
    pub rx_depth: Option<usize>,
    /// Transmit descriptor depth when exposed by a backend.
    pub tx_depth: Option<usize>,
}

/// Static or queue-local buffer capability facts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BufferCapabilities {
    /// Maximum packet length accepted by this buffer/backend.
    pub max_packet_len: usize,
    /// Maximum public headroom available without reallocation.
    pub max_headroom: usize,
    /// Maximum public tailroom available without reallocation.
    pub max_tailroom: usize,
    /// Maximum scatter-gather segment count.
    pub max_segments: usize,
    /// Whether the backing memory is DMA-capable.
    pub dma_capable: bool,
    /// Whether the backing memory is externally registered with a backend.
    pub externally_registered: bool,
}

/// Error returned by safe packet buffer accessors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum BufferAccessError {
    /// The requested offset/length is outside the current packet bytes.
    OutOfBounds {
        /// Requested byte offset.
        offset: usize,
        /// Requested byte length.
        len: usize,
        /// Current packet length.
        packet_len: usize,
    },
    /// The append operation did not fit in the available tailroom.
    ///
    /// This is a *buffer-layout* error raised while building a packet on the
    /// caller side. The wire-level "packet exceeds socket MTU" check on the
    /// transmit path is reported as [`crate::Error::OversizeForMtu`] instead.
    InsufficientTailroom {
        /// Available tailroom bytes.
        available: usize,
        /// Requested append bytes.
        requested: usize,
    },
}

impl fmt::Display for BufferAccessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutOfBounds {
                offset,
                len,
                packet_len,
            } => write!(
                f,
                "buffer access out of bounds: offset {offset}, length {len}, packet length {packet_len}"
            ),
            Self::InsufficientTailroom {
                available,
                requested,
            } => write!(
                f,
                "insufficient tailroom: requested {requested}, available {available}"
            ),
        }
    }
}

impl std::error::Error for BufferAccessError {}

/// Error returned by prefix and tail prepend/append operations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReserveError {
    /// Not enough public headroom to satisfy the request.
    InsufficientHeadroom {
        /// Available public headroom bytes.
        available: usize,
        /// Requested write bytes.
        requested: usize,
    },
    /// Not enough tailroom to satisfy the request.
    InsufficientTailroom {
        /// Available tailroom bytes.
        available: usize,
        /// Requested write bytes.
        requested: usize,
    },
    /// A tail write would cross a segment or fixed chunk boundary.
    SegmentBoundary,
    /// The buffer layout cannot expand in the requested direction.
    LayoutUnsupported,
}

impl fmt::Display for ReserveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InsufficientHeadroom {
                available,
                requested,
            } => write!(
                f,
                "insufficient headroom: requested {requested}, available {available}"
            ),
            Self::InsufficientTailroom {
                available,
                requested,
            } => write!(
                f,
                "insufficient tailroom: requested {requested}, available {available}"
            ),
            Self::SegmentBoundary => f.write_str("write would cross a segment boundary"),
            Self::LayoutUnsupported => f.write_str("buffer layout does not support this write"),
        }
    }
}

impl std::error::Error for ReserveError {}

/// Immutable packet buffer interface.
pub trait PacketBuffer {
    /// Segment iterator type returned by this buffer.
    type Segments<'a>: Iterator<Item = Segment<'a>>
    where
        Self: 'a;

    /// Returns the total packet length across all segments.
    fn len(&self) -> usize;

    /// Returns `true` when the packet has no bytes.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns public prefix space currently available before the packet.
    fn headroom(&self) -> usize;

    /// Returns public suffix space currently available after the packet.
    fn tailroom(&self) -> usize;

    /// Returns the layout used to allocate this buffer.
    fn layout(&self) -> &BufferLayout;

    /// Iterates packet segments in packet-byte order.
    fn segments(&self) -> Self::Segments<'_>;

    /// Reads exactly `dst.len()` bytes at `offset` across packet segments.
    fn read_at_exact(&self, offset: usize, dst: &mut [u8]) -> Result<(), BufferAccessError>;
}

/// Mutable packet buffer interface.
pub trait PacketBufferMut: PacketBuffer {
    /// Immutable buffer type produced by freezing this buffer.
    type Frozen: PacketBuffer;

    /// Prepends bytes immediately before the current packet start.
    ///
    /// Fails with `InsufficientHeadroom` if there is not enough headroom; the
    /// packet is left unchanged in that case.
    fn prepend(&mut self, bytes: &[u8]) -> Result<(), ReserveError>;

    /// Prepends bytes, relocating existing packet bytes when the implementation supports it.
    fn prepend_relocating(&mut self, bytes: &[u8]) -> Result<(), ReserveError> {
        self.prepend(bytes)
    }

    /// Appends bytes to the packet tail.
    fn extend_from_slice(&mut self, bytes: &[u8]) -> Result<(), BufferAccessError>;

    /// Appends bytes, relocating existing packet bytes when the implementation supports it.
    fn extend_from_slice_relocating(&mut self, bytes: &[u8]) -> Result<(), BufferAccessError> {
        self.extend_from_slice(bytes)
    }

    /// Trims bytes from the packet prefix.
    fn trim_prefix(&mut self, len: usize) -> Result<(), BufferAccessError>;

    /// Trims bytes from the packet suffix.
    fn trim_suffix(&mut self, len: usize) -> Result<(), BufferAccessError>;

    /// Freezes the mutable buffer into an immutable packet buffer.
    fn freeze(self) -> Self::Frozen;
}

/// Owned immutable packet buffer that can be converted back into mutable form.
///
/// This is useful for wrappers that accept frozen payload buffers at one API
/// layer and then need to prepend or append protocol headers before passing the
/// packet to a lower layer.
pub trait OwnedPacketBuffer: PacketBuffer + Sized {
    /// Mutable buffer type recovered from this owned immutable buffer.
    type Mutable: PacketBufferMut<Frozen = Self>;

    /// Converts this owned immutable packet back into mutable form.
    fn into_mut(self) -> Self::Mutable;
}

/// Shared buffer pool abstraction.
pub trait BufferPool {
    /// Mutable buffer type allocated by this pool.
    type Buffer: PacketBufferMut;

    /// Returns the layout used for newly allocated buffers.
    fn layout(&self) -> &BufferLayout;

    /// Allocates one mutable packet buffer.
    fn allocate(&mut self) -> Option<Self::Buffer>;
}

/// Immutable heap-backed packet buffer.
///
/// Move-only: cloning a buffer would copy the full packet bytes plus headroom
/// and tailroom, which is not appropriate on the steady-state packet path.
#[derive(Debug)]
pub struct PacketBuf {
    storage: Vec<u8>,
    start: usize,
    end: usize,
    layout: BufferLayout,
}

impl PacketBuf {
    /// Creates an immutable packet buffer by copying bytes into a compact layout.
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

    /// Returns the packet bytes as a contiguous slice.
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
        // start >= layout.l2_headroom() is upheld by every constructor and by
        // the operations on PacketBufMut (prepend/trim_prefix). saturating_sub
        // protects against a future constructor accidentally violating that.
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

/// Mutable heap-backed packet buffer.
///
/// Move-only: cloning a buffer would copy the full packet bytes plus headroom
/// and tailroom, which is not appropriate on the steady-state packet path.
#[derive(Debug)]
pub struct PacketBufMut {
    storage: Vec<u8>,
    start: usize,
    end: usize,
    layout: BufferLayout,
}

impl PacketBufMut {
    /// Creates an empty mutable buffer with the provided layout.
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

    /// Creates a mutable packet buffer by copying packet bytes into a compact layout.
    #[must_use]
    pub fn copy_from_slice(bytes: &[u8]) -> Self {
        let mut buffer = Self::new(BufferLayout::new(bytes.len()));
        buffer
            .extend_from_slice(bytes)
            .expect("fresh compact buffer has enough tailroom");
        buffer
    }

    /// Returns the packet bytes as a contiguous slice.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.storage[self.start..self.end]
    }

    /// Returns the packet bytes as a mutable contiguous slice.
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
        // See PacketBuf::headroom for the invariant rationale.
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

/// Heap-backed buffer pool used by tests, examples, and OS-copy paths.
///
/// This pool always produces single-segment, default-aligned [`PacketBufMut`]
/// values. The layout's [`BufferLayout::align`] and
/// [`BufferLayout::max_segments`] facts describe backend-honored requirements;
/// the heap pool reports them through [`BufferPool::layout`] but does not
/// allocate aligned memory and does not produce multi-segment buffers. Backends
/// with their own allocators (AF_XDP UMEM, DPDK mempools) are expected to
/// honor those fields.
#[derive(Debug)]
pub struct HeapBufferPool {
    layout: BufferLayout,
}

impl HeapBufferPool {
    /// Creates a heap buffer pool with the given layout.
    ///
    /// The layout is forced to a single segment because the heap pool always
    /// returns contiguous buffers; the rest of the layout facts pass through
    /// unchanged.
    #[must_use]
    pub fn new(layout: BufferLayout) -> Self {
        Self {
            layout: layout.with_max_segments(1),
        }
    }

    /// Creates a heap buffer pool for contiguous packets of `payload_capacity`.
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

#[cfg(test)]
mod tests {
    use super::*;

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
        let mut prefix_buffer = PacketBufMut::copy_from_slice(b"payload");
        prefix_buffer.prepend_relocating(b"head").unwrap();
        assert_eq!(prefix_buffer.as_slice(), b"headpayload");

        let mut tail_buffer = PacketBufMut::copy_from_slice(b"payload");
        tail_buffer.extend_from_slice_relocating(b"tail").unwrap();
        assert_eq!(tail_buffer.as_slice(), b"payloadtail");
    }

    #[test]
    fn fixed_chunk_layout_validates_bypass_facts() {
        let layout = BufferLayout::with_headroom_and_tailroom(128, 32, 16)
            .with_l2_headroom(14)
            .with_fixed_chunk(256, 512)
            .unwrap();

        assert_eq!(layout.chunk_size(), 256);
        assert_eq!(layout.stride(), 512);
        assert_eq!(layout.data_offset(), 46);

        assert!(matches!(
            BufferLayout::with_headroom_and_tailroom(128, 32, 16)
                .with_l2_headroom(14)
                .with_fixed_chunk(64, 64),
            Err(BufferLayoutError::ChunkTooSmall { .. })
        ));
    }

    #[test]
    fn fixed_chunk_survives_later_compatible_l2_headroom() {
        // Set fixed chunk first, then add a small l2 headroom that still fits.
        let layout = BufferLayout::with_headroom_and_tailroom(128, 32, 16)
            .with_fixed_chunk(256, 512)
            .unwrap()
            .with_l2_headroom(14);

        assert_eq!(layout.chunk_size(), 256);
        assert_eq!(layout.stride(), 512);
        assert_eq!(layout.data_offset(), 46);
    }

    #[test]
    #[should_panic(expected = "with_l2_headroom invalidates fixed chunk")]
    fn fixed_chunk_panics_on_incompatible_l2_headroom() {
        // Fixed-chunk = 192 leaves only 16 bytes of slack over the 32+128+16
        // layout. A 20-byte l2 headroom pushes minimum_chunk_size past 192.
        let _ = BufferLayout::with_headroom_and_tailroom(128, 32, 16)
            .with_fixed_chunk(192, 192)
            .unwrap()
            .with_l2_headroom(20);
    }
}
