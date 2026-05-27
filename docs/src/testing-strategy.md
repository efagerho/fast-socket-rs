# Testing Strategy

Testing should cover trait semantics, packet transformations, backend behavior,
and compile-time shape.

Core tests should verify:

- `TxSlot` ownership transfer and partial-send behavior;
- `RecvBatch` capacity, draining, and reuse;
- buffer layout invariants;
- prepend, append, and relocation error rules;
- trimming and bounded reads;
- route and egress resolver composition;
- direct IP packet routing and tunnel workload fit.

Backend tests should focus on backend boundaries. The OS backend can use
loopback sockets and pool checks. The XDP backend should keep unprivileged tests
for packet parsing, Ethernet normalization, route snapshots, route-monitor
fanout, ring-size validation, UMEM descriptor bounds, bulk ring wrap-around,
live TX batch allocation, cross-thread reclaim, embedded UDP filter maps, egress
validation, direct `XdpUdpSocket` send/receive behavior, RX/TX completion
reclaim routing, TX-pressure completion drains, NUMA parsing and validation, and
in-memory first-pass send/receive behavior.

Live backend tests need separation from ordinary CI. AF_XDP tests require Linux,
privileges, NIC queue setup, an attachable XDP program, and a controlled network
environment. NUMA-placement tests also need usable interface sysfs, `mbind`,
and `move_pages`. These tests should be opt-in and report skipped requirements
clearly.

Compile-time and codegen tests are part of the design. Useful checks include:

- type sizes for hot metadata and transmit items;
- absence of unwanted backend symbols in representative binaries;
- no dynamic dispatch in selected hot paths;
- feature-shape tests for optional metadata and policy specialization.

Performance tests should not replace correctness tests. A fast path that drops
the wrong packet, accepts skipped transmit slots, or leaks UMEM frames is still
incorrect.
