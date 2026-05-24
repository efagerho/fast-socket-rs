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

Backend tests should focus on the backend boundary. The OS backend can be tested
with loopback sockets and pool behavior. The XDP backend should keep
unprivileged tests for packet parsing, Ethernet normalization, route snapshot
resolution, route-monitor fanout, ring-size validation, UMEM descriptor bounds,
bulk ring cursor reservation and wrap-around descriptor copies, live TX batch
allocation, cross-thread buffer drop reclaim, embedded UDP filter-map
validation, egress validation, direct `XdpUdpSocket` send/receive behavior,
RX/TX completion reclaim routing, TX-pressure completion drain behavior,
NUMA-node parsing and configuration validation, and in-memory first-pass
send/receive behavior.

Live backend tests need clear separation from ordinary CI. AF_XDP tests require
Linux, privileges, NIC queue setup, an attachable XDP program, and a controlled
network environment. NUMA-placement tests also need usable interface sysfs,
`mbind`, and `move_pages` support. Those tests should be opt-in and should
report skipped requirements clearly.

Compile-time and codegen tests are part of the design. Useful checks include:

- type sizes for hot metadata and transmit items;
- absence of unwanted backend symbols in representative binaries;
- no dynamic dispatch in selected hot paths;
- feature-shape tests for optional metadata and policy specialization.

Performance tests should not replace correctness tests. A fast packet path that
silently drops the wrong packet, accepts skipped transmit slots, or leaks UMEM
frames is incorrect no matter how good its throughput looks.
