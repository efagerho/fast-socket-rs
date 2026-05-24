# RawDevice

`RawDevice` is an optional side API for device-backed sockets. It exposes
device facts that are useful for setup, monitoring, and diagnostics, but it is
not required for sending or receiving packets.

The trait reports:

- the operating-system interface index;
- static device capabilities;
- queue CPU-affinity hints;
- queue NUMA-node hints;
- cumulative per-device or per-queue statistics;
- refreshed MTU after administrative changes.

Keeping this separate from `IpPacketSocket` matters. A simple IP packet path
should not need to carry device-control branches, and generic UDP code should
not inherit device APIs unless the caller explicitly asks for them through a
concrete backend type.

`RawDeviceStats` contains counters that are useful for understanding packet-path
behavior: received and transmitted packets and bytes, dropped IP fragments,
oversize transmit attempts, and ring-full events.

Those counters are separate from ownership bookkeeping. For example, AF_XDP
completion draining may reclaim both TX-pool frames and RX frames that need to
return to FILL; the returned completion count tells how many descriptors were
consumed, while the socket keeps the underlying frame ownership internal.

`Capabilities` describes coarse hardware or backend features such as checksum
offload, RSS, TSO, GRO, tunnel-aware RSS, timestamping, and inline security.
Capabilities are facts about what the device/backend can expose; they are not a
promise that every socket path will use those features automatically.

The AF_XDP IP packet and direct UDP sockets implement `RawDevice` today. Their
current capability bitset is conservative, while their statistics already
reflect receive, transmit, oversize, fragment-drop, and ring-full behavior. For
live sockets, `queue_numa_node()` reports the NUMA node selected and verified
for UMEM placement; first-pass sockets report the configured hint when one
exists.
