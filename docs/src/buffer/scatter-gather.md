# Scatter-Gather Buffers

The buffer traits support scatter-gather packet storage even though the current
heap, OS, and XDP buffers are single-segment.

`PacketBuffer::segments()` returns packet segments in packet-byte order.
`PacketBuffer::len()` is the total length across all segments. `read_at_exact`
must work across segment boundaries so header parsing code can avoid assuming
contiguous storage.

`ScatterGather` is a borrowed view over a list of packet segments. It is useful
for APIs that need to pass a packet to lower-level vectored I/O without
flattening it.

Multi-segment backends must preserve the same logical packet semantics:

- segment iteration order is byte order;
- reads by offset see one continuous packet;
- prepend and append operations must either fit within supported layout rules or
  fail with a precise buffer error;
- transmit ownership still moves as one packet item.

When a backend cannot support a write across a segment or chunk boundary,
it should return `ReserveError::SegmentBoundary` or `LayoutUnsupported` instead
of silently copying through an unexpected path.
