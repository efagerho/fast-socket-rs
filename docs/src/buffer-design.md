# Buffer Design

The buffer model makes packet memory visible enough for high-performance
backends without forcing every backend to use the same allocator.

`BufferPool` allocates mutable packet buffers. A mutable buffer implements
`PacketBufferMut`, and freezing it produces the immutable buffer type used for
transmit. Some immutable buffers also implement `OwnedPacketBuffer`, allowing a
wrapper to recover mutable ownership when it needs to prepend or append headers.

The declarations below show the actual buffer traits, with default function
bodies omitted:

```rust,ignore
pub type Segment<'a> = &'a [u8];

pub trait PacketBuffer {
    type Segments<'a>: Iterator<Item = Segment<'a>>
    where
        Self: 'a;

    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn headroom(&self) -> usize;
    fn tailroom(&self) -> usize;
    fn layout(&self) -> &BufferLayout;
    fn segments(&self) -> Self::Segments<'_>;
    fn read_at_exact(&self, offset: usize, dst: &mut [u8])
        -> Result<(), BufferAccessError>;
}

pub trait PacketBufferMut: PacketBuffer {
    type Frozen: PacketBuffer;

    fn prepend(&mut self, bytes: &[u8]) -> Result<(), ReserveError>;
    fn prepend_relocating(&mut self, bytes: &[u8]) -> Result<(), ReserveError>;
    fn extend_from_slice(&mut self, bytes: &[u8])
        -> Result<(), BufferAccessError>;
    fn extend_from_slice_relocating(&mut self, bytes: &[u8])
        -> Result<(), BufferAccessError>;
    fn trim_prefix(&mut self, len: usize) -> Result<(), BufferAccessError>;
    fn trim_suffix(&mut self, len: usize) -> Result<(), BufferAccessError>;
    fn freeze(self) -> Self::Frozen;
}

pub trait OwnedPacketBuffer: PacketBuffer + Sized {
    type Mutable: PacketBufferMut<Frozen = Self>;

    fn into_mut(self) -> Self::Mutable;
}

pub trait BufferPool {
    type Buffer: PacketBufferMut;

    fn layout(&self) -> &BufferLayout;
    fn allocate(&mut self) -> Option<Self::Buffer>;
}
```

The split is intentional. `PacketBuffer` is the immutable packet view used by
parsers and transmit paths. It exposes logical packet length, layout facts, and
ordered segment iteration without requiring contiguous storage.

`PacketBufferMut` is available only while the caller owns mutable packet memory.
It changes the visible packet range through prepend, append, trim, and freeze
operations. The relocating variants have default non-relocating behavior, so a
backend must explicitly choose to move bytes when it can support that cold path.

`OwnedPacketBuffer` is separate from `PacketBuffer` because not every immutable
packet can safely be turned back into mutable storage. Future adapters can
require owned immutable buffers when they need to recover mutable ownership and
add protocol headers.

`BufferPool` is deliberately small: it reports the layout for future
allocations and hands out mutable buffers. Socket traits own the pools so
allocation remains queue-local and backend-specific.

`BufferLayout` describes the memory shape:

- payload capacity;
- public headroom and tailroom;
- backend-reserved L2 headroom;
- fixed chunk size and stride;
- data alignment;
- maximum segment count.

This is enough to describe heap buffers, AF_XDP UMEM frames, and future
DPDK-style mbufs with one vocabulary.

The core heap buffer pool is a small reference implementation. The OS backend
has its own slab-backed queue-local pool to avoid allocator churn. The XDP
backend has UMEM-backed live pools and heap fallback pools for unprivileged
tests. Live XDP UMEM is allocated on the selected queue NUMA node and verifies
page placement during setup so packet memory does not silently drift to a remote
node.

The traits do not require every buffer to be contiguous, but the current
implementations expose one segment. Scatter-gather support is still part of the
trait because real backends may eventually need multi-segment packet storage.
