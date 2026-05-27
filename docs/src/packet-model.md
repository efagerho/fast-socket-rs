# Packet Model

Fast Socket uses explicit packet boundaries. Packet-buffer bytes are
authoritative at each API layer. Metadata can route, validate, or optimize an
operation, but it does not replace missing bytes.

There are two public packet boundaries:

- UDP sockets carry UDP payload bytes.
- IP packet sockets carry complete IP datagrams.

This division keeps UDP simple while preserving enough detail for forwarding and
kernel-bypass backends. UDP callers do not build IP or UDP headers. IP packet
callers provide an IP datagram, but omit Ethernet, VLAN, ARP, and other
link-layer data.

Backends may use lower-level internal boundaries. AF_XDP receives and transmits
Ethernet frames at the NIC. Its public `IpPacketSocket` trims receives to the IP
header and prepends Ethernet headers from `XdpEgress` on transmit. Its
`UdpSocket` also trims or prepends IPv4 and UDP headers so callers see only UDP
payloads.
