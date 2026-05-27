# Headroom and Tailroom

Headroom and tailroom let protocol layers add bytes without reallocating or
copying existing payload bytes.

Public headroom is space before the packet start. Public tailroom is space
after the packet end. `BufferLayout` can also reserve backend-owned L2
headroom. AF_XDP uses that split so public IP packets start at the IP header
while the backend still has room for Ethernet or VLAN headers.

`PacketBufferMut` exposes direct prepend and append operations:

- `prepend(bytes)` writes bytes immediately before the current packet start.
- `extend_from_slice(bytes)` appends bytes immediately after the current packet
  end.

Both operations make bytes visible only after the full slice is copied. The API
has no sparse reservation guard or separate commit step.

`prepend_relocating` and `extend_from_slice_relocating` allow byte movement when
the layout lacks enough headroom or tailroom. By default they delegate to the
non-relocating operation. Backends should keep relocation cold and measurable
because it can allocate or copy payload bytes.

`trim_prefix` and `trim_suffix` remove bytes from the visible packet. XDP uses
them to normalize Ethernet frames into IP datagrams, and direct `XdpUdpSocket`
uses them to expose UDP payloads.
