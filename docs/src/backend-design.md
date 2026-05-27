# Backend Design

Backend crates turn platform mechanisms into core socket traits. They preserve
public invariants even when the underlying API has a different packet boundary
or ownership model.

A backend defines:

- socket builder and configuration types;
- receive and transmit buffer pools;
- concrete packet buffer types;
- polling driver selection;
- receive metadata;
- completion and transmit-notification behavior;
- error mapping into the core `Error` vocabulary.

Backends normalize their packet boundary to the trait they implement. The OS
backend implements `UdpSocket`, so it exposes UDP payloads. The XDP IP packet
socket implements `IpPacketSocket`, so it exposes IP datagrams even though
AF_XDP works with Ethernet frames. The XDP UDP socket implements `UdpSocket`
directly and exposes UDP payloads while reusing AF_XDP queue machinery.

Backends should expose copy boundaries. If the OS kernel copies received UDP
bytes into user memory, benchmarks should show it. If an XDP socket wraps a UMEM
frame, completion and frame reclamation must be explicit.

The preferred backend shape is queue-local ownership. A socket owns one logical
queue, its buffer pools, its polling driver, and its local state. This supports
CPU affinity, NUMA placement, per-queue routing snapshots, and non-atomic memory
recycling.

Backend-specific features belong on concrete types or optional side traits. They
should not force generic callers to carry fields or branches for one backend.
