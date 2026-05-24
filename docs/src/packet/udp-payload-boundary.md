# UDP Payload Boundary

The `UdpSocket` packet boundary is the UDP payload. The buffer passed to
`UdpTransmit` contains only application payload bytes, and the buffer delivered
by `UdpReceive` contains only received payload bytes.

Socket-address metadata carries the transport endpoints. `UdpTransmit` has a
required destination address and optional source IP selection. `UdpRecvMeta`
reports the remote source, and may report the local destination IP when the
backend exposes it.

For a direct OS backend, this maps naturally onto the operating system's UDP
socket API. The kernel builds and parses the IP and UDP headers.

For direct AF_XDP UDP, `XdpUdpSocket` performs that work in the backend. It
builds IPv4/UDP and Ethernet headers on transmit. In live mode it parses
Ethernet, IPv4, and UDP in one backend pass; in first-pass tests it parses
normalized IPv4 UDP datagrams. In both modes it exposes only the UDP payload at
the `UdpSocket` boundary.

The current direct AF_XDP UDP socket is IPv4-only. It builds minimal IPv4 UDP
datagrams, computes the IPv4 header checksum, records ECN in the IPv4 TOS byte,
and drops non-UDP, fragmented, malformed, or wrong-destination packets during
receive parsing.
