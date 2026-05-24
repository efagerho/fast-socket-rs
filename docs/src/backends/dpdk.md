# Future DPDK Backend

The DPDK backend is intentionally future work. The core API is shaped so that a
DPDK implementation should fit, but the project should not pretend to know all
DPDK-specific requirements before one exists.

The likely DPDK backend shape is:

- implement `IpPacketSocket` as the primary packet API;
- likely implement `UdpSocket` directly for optimized UDP workloads;
- expose a DPDK-specific egress handle implementing `IpPacketEgress`;
- use mbuf-backed buffer pools;
- map RX and TX queues to queue-local socket values;
- implement explicit completion or reclaim behavior appropriate for the TX
  path;
- optionally implement `RawDevice` for port capabilities and statistics.

The existing IP packet boundary should still hold. DPDK may receive and transmit
Ethernet frames at the device, but the public `IpPacketSocket` should expose
complete IP datagrams. The backend would normalize received frames by trimming
L2 headers and would prepend L2 headers during transmit from a resolved egress
handle.

A direct DPDK `UdpSocket` would expose UDP payloads instead. Like `XdpUdpSocket`,
it should keep UDP parsing, header construction, offloads, and mbuf details in
the backend-specific path rather than relying on a generic UDP-over-IP adapter.

Several questions should wait for implementation pressure:

- how to represent multi-segment mbufs through the existing buffer traits;
- whether public headroom and backend L2 headroom are enough for all required
  encapsulation layouts;
- which DPDK offloads should appear as core `TxOffload` flags versus
  backend-specific configuration;
- how port configuration, RSS, mempool setup, and EAL arguments should be
  exposed without polluting the core crate;
- whether flow offload or SmartNIC match/action programming needs a separate
  API.

The first DPDK pass should be conservative: implement IP packet I/O, prove the
buffer model, add direct UDP only when the backend-specific path is clear, and
only then promote shared concepts into the core crate.
