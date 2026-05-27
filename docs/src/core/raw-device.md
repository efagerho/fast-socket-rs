# RawDevice

`RawDevice` is an optional side API for device-backed sockets. It exposes setup,
monitoring, and diagnostic facts, but send and receive do not require it.

The trait reports:

- the operating-system interface index;
- static device capabilities;
- queue CPU-affinity hints;
- queue NUMA-node hints;
- cumulative per-device or per-queue statistics;
- refreshed MTU after administrative changes.

Keeping this separate from `IpPacketSocket` keeps device-control branches out of
simple IP packet paths. Generic UDP code gets device APIs only through concrete
backend types.

`RawDeviceStats` contains packet-path counters: received and transmitted packets
and bytes, dropped IP fragments, oversize transmit attempts, and ring-full
events.

Those counters are separate from ownership bookkeeping. AF_XDP completion
draining may reclaim TX-pool frames and RX frames that must return to FILL. The
returned completion count reports consumed descriptors; the socket keeps frame
ownership internal.

`Capabilities` describes coarse hardware or backend features: checksum offload,
RSS, TSO, GRO, tunnel-aware RSS, timestamping, and inline security. These are
available facts, not a promise that every socket path uses them.

The AF_XDP IP packet and UDP sockets implement `RawDevice` today. Their
capability bitset is conservative, while their statistics cover receive,
transmit, oversize, fragment-drop, and ring-full behavior. Live sockets report
the NUMA node selected and verified for UMEM placement; first-pass sockets
report the configured hint when present.
