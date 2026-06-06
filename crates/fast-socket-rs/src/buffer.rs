//! Shared packet buffer traits and layout types.

use core::fmt;
use core::num::NonZeroUsize;

/// A borrowed packet segment.
pub type Segment<'a> = &'a [u8];

/// Iterator over packet segments in packet-byte order.
///
/// This alias is useful for single-segment packet buffers.
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

/// Buffer layout facts shared by pools and queues.
///
/// Field invariants:
///
/// - `data_offset == l2_headroom + headroom`
/// - `chunk_size >= data_offset + payload_capacity + tailroom`
/// - `stride >= chunk_size`
///
/// `align` and `max_segments` are descriptive facts for allocators and queues.
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
    /// buffer comfortably holds an MTU-sized Ethernet frame after link-layer
    /// headroom is added.
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
        let chunk_size = headroom + payload_capacity + tailroom;
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

    /// Returns a copy of this layout with link-layer headroom.
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
    /// This is useful for describing fixed-size packet memory. `chunk_size`
    /// must fit the configured L2 headroom, public headroom, payload capacity,
    /// and tailroom. `stride` must be at least as large as the chunk size.
    ///
    /// # Panics
    ///
    /// Panics if `chunk_size` is smaller than the layout minimum, or if `stride`
    /// is smaller than `chunk_size`. Consistent with the other `BufferLayout`
    /// builder methods ([`Self::with_alignment`], [`Self::with_max_segments`],
    /// [`Self::with_l2_headroom`]), an invalid layout is a construction-time
    /// programmer error and panics rather than returning a `Result`.
    #[must_use]
    pub fn with_fixed_chunk(mut self, chunk_size: usize, stride: usize) -> Self {
        let minimum = self.minimum_chunk_size();
        assert!(
            chunk_size >= minimum,
            "with_fixed_chunk chunk_size {chunk_size} is smaller than the layout minimum {minimum}",
        );
        assert!(
            stride >= chunk_size,
            "with_fixed_chunk stride {stride} is smaller than the chunk size {chunk_size}",
        );

        self.chunk_size = chunk_size;
        self.stride = stride;
        self.chunk_fixed = true;
        self
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

    /// Link-layer headroom before the public headroom.
    #[must_use]
    pub const fn l2_headroom(self) -> usize {
        self.l2_headroom
    }

    /// Total contiguous allocation length needed for one packet chunk.
    #[must_use]
    pub const fn allocation_len(self) -> usize {
        self.chunk_size
    }

    const fn minimum_chunk_size(self) -> usize {
        self.data_offset + self.payload_capacity + self.tailroom
    }
}

/// Per-queue buffer layout and depth configuration.
#[derive(Clone, Copy, Debug)]
pub struct QueueBufferConfig {
    /// Receive buffer layout.
    pub rx: BufferLayout,
    /// Transmit buffer layout.
    pub tx: BufferLayout,
    /// Receive queue depth when known.
    pub rx_depth: Option<usize>,
    /// Transmit queue depth when known.
    pub tx_depth: Option<usize>,
}

/// Static or queue-local buffer capability facts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BufferCapabilities {
    /// Maximum packet length accepted by this buffer configuration.
    pub max_packet_len: usize,
    /// Maximum public headroom available without reallocation.
    pub max_headroom: usize,
    /// Maximum public tailroom available without reallocation.
    pub max_tailroom: usize,
    /// Maximum scatter-gather segment count.
    pub max_segments: usize,
    /// Whether the backing memory is DMA-capable.
    pub dma_capable: bool,
    /// Whether the backing memory is externally registered.
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
    /// caller side. The transmit-path MTU check is reported as
    /// [`crate::Error::OversizeForMtu`] instead.
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

/// Buffer pool suitable for socket traits.
///
/// Socket receive and transmit buffers can cross worker-thread boundaries as
/// owned values. A socket buffer pool must therefore hand out mutable buffers
/// that are [`Send`], and freezing those buffers must also produce a [`Send`]
/// immutable buffer.
pub trait SocketBufferPool: BufferPool
where
    Self::Buffer: Send,
    <Self::Buffer as PacketBufferMut>::Frozen: Send,
{
}

impl<P> SocketBufferPool for P
where
    P: BufferPool,
    P::Buffer: Send,
    <P::Buffer as PacketBufferMut>::Frozen: Send,
{
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_chunk_layout_validates_bypass_facts() {
        let layout = BufferLayout::with_headroom_and_tailroom(128, 32, 16)
            .with_l2_headroom(14)
            .with_fixed_chunk(256, 512);

        assert_eq!(layout.chunk_size(), 256);
        assert_eq!(layout.stride(), 512);
        assert_eq!(layout.data_offset(), 46);
    }

    #[test]
    #[should_panic(expected = "smaller than the layout minimum")]
    fn fixed_chunk_too_small_panics() {
        let _ = BufferLayout::with_headroom_and_tailroom(128, 32, 16)
            .with_l2_headroom(14)
            .with_fixed_chunk(64, 64);
    }

    #[test]
    fn fixed_chunk_survives_later_compatible_l2_headroom() {
        // Set fixed chunk first, then add a small l2 headroom that still fits.
        let layout = BufferLayout::with_headroom_and_tailroom(128, 32, 16)
            .with_fixed_chunk(256, 512)
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
            .with_l2_headroom(20);
    }
}
