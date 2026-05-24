# Packet Model

Fast Socket uses explicit packet boundaries. The bytes in the packet buffer are
the authoritative representation of the packet at that API layer. Metadata can
help route, validate, or optimize the operation, but metadata does not replace
bytes that are missing from the buffer.

There are two public packet boundaries:

- UDP sockets carry UDP payload bytes.
- IP packet sockets carry complete IP datagrams.

This division keeps the high-level API pleasant while preserving enough detail
for forwarding and kernel-bypass backends. A UDP caller does not need to build
IP or UDP headers. An IP packet caller does need to provide an IP datagram, but
does not need to include Ethernet, VLAN, ARP, or other link-layer data in the
core packet type.

Backends may have lower-level internal boundaries. AF_XDP receives Ethernet
frames from the NIC and transmits Ethernet frames back to it. Its public
`IpPacketSocket` implementation trims received frames to the IP header and
prepends Ethernet headers during transmit from the resolved `XdpEgress`.
Its direct `UdpSocket` implementation additionally trims or prepends IPv4 and
UDP headers so callers see only UDP payloads.
