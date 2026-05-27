# Scatter-Gather Buffers

The buffer traits support scatter-gather storage, though current heap, OS, and
XDP buffers are single-segment.

`PacketBuffer::segments()` returns segments in packet-byte order.
`PacketBuffer::len()` is the total length across segments. `read_at_exact` must
work across segment boundaries so parsers need not assume contiguous storage.

`ScatterGather` is a borrowed view over packet segments for vectored I/O without
flattening.

Multi-segment backends must preserve the same logical packet semantics:

- segment iteration order is byte order;
- reads by offset see one continuous packet;
- prepend and append operations must either fit within supported layout rules or
  fail with a precise buffer error;
- transmit ownership still moves as one packet item.

When a backend cannot write across a segment or chunk boundary, it should return
`ReserveError::SegmentBoundary` or `LayoutUnsupported` instead of silently
copying through an unexpected path.
