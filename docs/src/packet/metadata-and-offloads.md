# Metadata and Offloads

Metadata is descriptive or steering-related. Packet bytes remain authoritative.

For UDP receive, `UdpRecvMeta` records:

- remote source socket address;
- optional local destination IP;
- optional ECN codepoint;
- UDP payload length;
- optional GRO stride for coalesced receives.

For IP packet receive, `IpPacketRecvMeta` records:

- IP version;
- complete IP datagram length;
- L4 checksum status when known.

`ChecksumStatus` distinguishes verified, bad, unverified, and not-checked
receive checksums. A backend should use `NotChecked` when it did not ask the NIC
or kernel to validate the checksum.

Transmit offloads are explicit. `TxOffload` can request IPv4 checksum offload,
L4 checksum offload, or TSO. `IpPacketTransmit` also carries an optional TSO segment
size. `UdpTransmit` carries an optional GSO segment size.

The capabilities types describe what a socket can support:

- `UdpCapabilities` reports GSO and GRO support and an optional maximum GSO
  segment count.
- `Capabilities` on `RawDevice` reports device-level features such as checksum
  offload, RSS, TSO, GRO, timestamping, and inline security.

Backends should reject unsupported per-packet options with `InvalidPacket`
rather than silently pretending they were honored. The current OS UDP backend
rejects unsupported UDP transmit options, and the current XDP IP packet path
validates the IP datagram against the requested ethertype before transmit.
