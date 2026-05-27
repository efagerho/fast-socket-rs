# Open Questions

This chapter tracks questions until implementation experience answers them.

IPv6 direct UDP on kernel-bypass backends

The current direct `XdpUdpSocket` path is IPv4-only. IPv6 likely needs a
separate backend path or type-level policy so IPv4-only code keeps compiling
without IPv6 branches. The design needs checksum behavior, extension-header
handling, and MTU rules.

Checksum and segmentation offloads

The core has `TxOffload`, UDP GSO metadata, and capability flags, but backend
support is conservative. The project needs rules for software checksums,
hardware offload requests, and unsupported combinations.

Multi-segment buffers

The traits support segment iteration, but current buffers are single-segment.
DPDK or future XDP features may require stricter multi-segment prepend, append,
and relocation rules.

Tunnel support

The core route vocabulary includes tunnel tables, but there is no complete
tunnel adapter yet. The design needs one real consumer before adding more
surface area.

DPDK backend shape

The future DPDK backend should validate the IP packet boundary, direct UDP
socket shape, mbuf ownership, completion behavior, port configuration, and
queue-local design. Shared abstractions should move into the core only after
that implementation proves they are backend-agnostic.

Flow offload

SmartNIC or hardware match/action programming is out of scope. Revisit it only
when there are at least two consumers and a clear separation from packet I/O.

Benchmark gates

The project still needs thresholds for throughput, allocation count, copy count,
code size, and symbol-shape regressions. Introduce these gates carefully so CI
stays reliable.
