# Headroom and Tailroom

Headroom and tailroom let protocol layers add bytes without reallocating or
copying the existing payload.

Public headroom is space before the current packet start. Public tailroom is
space after the current packet end. `BufferLayout` can also reserve L2 headroom
that belongs to the backend. AF_XDP uses that distinction so the public IP
packet can start at the IP header while the backend still has room to prepend an
Ethernet or VLAN header before transmit.

`PacketBufferMut` exposes direct prepend and append operations:

- `prepend(bytes)` writes bytes immediately before the current packet start.
- `extend_from_slice(bytes)` appends bytes immediately after the current packet
  end.

Both operations make the bytes visible only after the full slice has been
copied into the buffer. There is no sparse reservation guard or separate commit
step in the current API.

`prepend_relocating` and `extend_from_slice_relocating` allow an implementation
to move packet bytes when the original layout lacks enough headroom or tailroom.
The default behavior delegates to the non-relocating operation. Backends should
keep relocation on a cold path and make it measurable, because it can allocate
or copy payload bytes.

`trim_prefix` and `trim_suffix` remove bytes from the visible packet. XDP uses
them to normalize Ethernet frames into IP datagrams, and direct `XdpUdpSocket`
uses them to expose UDP payloads.
