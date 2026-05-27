# Benchmarking

Benchmarks should test design promises, not only headline throughput.

Each meaningful run should record:

- git commit and dirty tree status;
- Rust toolchain, target, profile, features, and relevant flags;
- OS and kernel version;
- CPU model, core topology, SMT, governor, and boost state;
- NUMA topology;
- NIC model, driver, firmware, MTU, RSS, offloads, queues, and IRQ affinity;
- backend configuration such as socket options, AF_XDP mode, UMEM layout, ring
  depths, and queue mapping;
- thread pinning, queue-to-core mapping, and verified packet-memory NUMA
  placement for live kernel-bypass backends.

Runtime metrics should include packets per second, bytes per second, latency
percentiles, cycles per packet, instructions per packet, branch misses, cache
misses, syscalls per packet, allocations per packet, copied payload bytes,
batch-length distributions, completion counts, reclaim latency, ring-full
events, short accepts, oversize drops, fragment drops, and device drops.

Timing windows should match the measurement. Setup work such as socket opening,
XDP program attachment, route snapshot creation, worker pinning, and readiness
synchronization should be excluded from steady-state throughput unless setup is
the target. The XDP sender waits for worker sockets to open and signal readiness
before measurement. It captures elapsed time before shutdown joins and avoids a
final partial one-second stats interval after stop.

Load generators must not dominate the measured path. The XDP blast sender
submits UDP packets in fixed-size batches and publishes sent counters per batch,
while preserving prefix-accept semantics and sequence numbering for unaccepted
tails.

Benchmark tiers:

`Tier 0: static and codegen checks`

These checks prove unused features do not appear in representative optimized
symbols. Examples include verifying that direct OS UDP does not instantiate IP
packet routing and that no-op polling or completion paths inline away.

`Tier 1: core microbenchmarks`

These run without network I/O. They measure buffer allocation, freeze, prepend,
append, trimming, segment iteration, header reads, relocation, and batch
container behavior.

`Tier 2: backend socket benchmarks`

These measure OS and AF_XDP send/receive loops under controlled queue,
affinity, MTU, and batch-size settings.

`Tier 3: system benchmarks`

These use host-to-host or hardware traffic to measure throughput, latency,
drops, interrupts, and queue behavior under realistic load.

Profiling helpers live beside the benchmark crate. `benchmarks/profile-os.sh`
profiles the OS listener while a matching sender drives load.
`benchmarks/profile-xdp.sh` profiles the XDP UDP sender in `blast` or `ping`
mode. Both helpers require `perf` and root or non-interactive sudo. The XDP
helper builds the release sender, writes `run.env`, captures before/after NIC
stats with `ethtool -S` when interfaces are configured, captures `sender.log`,
`perf.data`, perf reports, and `perf.script`, and emits flamegraph artifacts
when Inferno or Brendan Gregg's FlameGraph scripts are available. If flamegraph
tools are missing, it still writes the raw perf script and a short conversion
note. The OS helper captures the same NIC stats around warmup and profiling.

Correctness tests should remain separate from performance gates. Assembly and
symbol checks are toolchain-sensitive, so they should be opt-in or pinned in CI.
