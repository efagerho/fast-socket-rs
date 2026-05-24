# Backend Design

Backend crates turn concrete platform mechanisms into the core socket traits.
They should preserve the public invariants even when the underlying API has a
different packet boundary or ownership model.

A backend implementation is expected to define:

- socket builder and configuration types;
- receive and transmit buffer pools;
- concrete packet buffer types;
- polling driver selection;
- receive metadata;
- completion and transmit-notification behavior;
- error mapping into the core `Error` vocabulary.

Backends should normalize their packet boundary to the trait they implement.
The OS backend implements `UdpSocket`, so it exposes UDP payloads. The XDP IP
packet socket implements `IpPacketSocket`, so it exposes complete IP datagrams
even though AF_XDP itself works with Ethernet frames. The XDP UDP socket
implements `UdpSocket` directly and exposes UDP payloads while reusing the same
AF_XDP queue machinery.

Backends should also be honest about copies. If the OS kernel copies received
UDP bytes into user memory, the backend should make that visible in benchmark
results rather than hiding it behind an API that looks zero-copy. If an XDP
socket wraps a UMEM frame directly, completion and frame reclamation must be
explicit and safe.

The preferred backend shape is queue-local ownership. A socket owns one logical
queue, its buffer pools, its polling driver, and its local state. That makes CPU
affinity, NUMA placement, per-queue routing snapshots, and non-atomic memory
recycling practical.

Backend-specific power features belong on concrete types or optional side
traits. They should not force every generic caller to carry fields or branches
that are only meaningful for one backend.
