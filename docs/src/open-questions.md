# Open Questions

This chapter tracks questions that should remain visible until implementation
experience answers them.

IPv6 direct UDP on kernel-bypass backends

The current direct `XdpUdpSocket` path is IPv4-only. IPv6 should likely be a
separate concrete backend path or type-level policy so IPv4-only code keeps
compiling without IPv6 branches. The design needs clear checksum behavior,
extension-header handling, and MTU rules.

Checksum and segmentation offloads

The core has `TxOffload`, UDP GSO metadata, and capability flags, but backend
support is still conservative. The project needs concrete rules for when to
compute checksums in software, when to request hardware offload, and how to
report unsupported combinations.

Multi-segment buffers

The traits support segment iteration, but current buffers are single-segment.
DPDK or future XDP features may force stricter multi-segment prepend, append,
and relocation rules.

Tunnel support

The core route vocabulary includes tunnel tables, but there is no complete
tunnel adapter yet. The design needs one real consumer before adding more
surface area.

DPDK backend shape

The future DPDK backend should validate the IP packet boundary, direct UDP
socket shape, mbuf ownership, completion behavior, port configuration, and
queue-local design. Shared abstractions should move into the core only after
that implementation proves they are not DPDK-specific.

Flow offload

SmartNIC or hardware match/action programming is out of scope for the current
plan. It should be revisited only when there are at least two concrete consumers
and a clear separation from ordinary packet I/O.

Benchmark gates

The project still needs stable thresholds for throughput, allocation count,
copy count, code size, and symbol-shape regressions. These gates should be
introduced carefully so ordinary CI remains reliable.
