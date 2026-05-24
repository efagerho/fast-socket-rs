# IP Packet Boundary

The `IpPacketSocket` packet boundary is a complete IP datagram. Byte zero is the
first byte of the IPv4 or IPv6 header.

This is the most important packet-model decision in the project. It means an IP
packet item is not:

- an Ethernet frame;
- a UDP payload;
- a transport payload;
- metadata that asks the backend to synthesize the IP header later.

The IP header is part of the packet bytes. Metadata may duplicate selected
facts, such as source address, destination address, hop limit, checksum status,
or egress, but it is not the source of truth for a missing header.

This boundary has several advantages:

- Future adapters can construct outer packets by prepending headers to an
  existing payload or inner datagram.
- Forwarding code can operate on IPv4 and IPv6 datagrams without pulling
  link-layer state into the core trait.
- AF_XDP and future DPDK backends can normalize Ethernet frames internally.
- ARP, neighbor resolution, VLAN tagging, and source MAC selection stay in
  backend egress resolution rather than the core packet I/O API.

If the project later needs a true link-layer API, it should be a separate
lower-level abstraction. It should not replace the main IP packet socket boundary.
