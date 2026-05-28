#!/usr/bin/env bash
# Drives the XDP sender under four perf recording modes back to back:
#   1. cycles:pp                 - precise per-instruction cycle attribution
#   2. L1-dcache-load-misses:pp  - precise per-instruction L1d miss attribution
#   3. ibs_op/ldlat=50/p         - load-latency profiling (AMD IBS Op)
#   4. perf c2c                  - cache-to-cache / false-sharing detection
#
# Each run is recorded to its own subdirectory under
# bench-results/analysis/<timestamp>/ and the per-mode post-processing
# (annotate, script, c2c report) is generated automatically.
#
# Environment variables match benchmarks/profile-xdp.sh.

set -euo pipefail

usage() {
  cat <<'EOF'
usage: benchmarks/analyze-xdp.sh [--mode blast|ping]
       [--symbols sym1,sym2,...] [--skip cycles,l1miss,ldlat,c2c]

Defaults match profile-xdp.sh. Override with env vars:
  PROFILE_SECONDS=30, DURATION_MS, PAYLOAD_LEN, IFACE, QUEUE_MODE,
  QUEUE, LOCAL, TARGET, RATE, STATS_IFACES, ETHTOOL, PERF, PERF_SUDO,
  PERF_FREQ, CALL_GRAPH, PERF_DELAY_MS, OUT_BASE.

--symbols selects functions for per-mode `perf annotate --stdio` output.
  Default: build_xdp_udp_transmit, allocate_many, drain_tx_completions_inner,
  drain_completion_slices, allocate_tx_batch_inner, run_blast_worker.

--skip omits one or more recording modes by name (comma-separated).
EOF
}

MODE=blast
ANNOTATE_SYMBOLS="build_xdp_udp_transmit,allocate_many,drain_tx_completions_inner,drain_completion_slices,allocate_tx_batch_inner,run_blast_worker"
SKIP=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    --symbols)
      ANNOTATE_SYMBOLS="${2:-}"
      shift 2
      ;;
    --skip)
      SKIP="${2:-}"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

case "$MODE" in
  blast|ping) ;;
  *)
    echo "--mode must be one of: blast, ping" >&2
    exit 2
    ;;
esac

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE_SECONDS="${PROFILE_SECONDS:-30}"
DURATION_MS="${DURATION_MS:-$((PROFILE_SECONDS * 1000))}"
PAYLOAD_LEN="${PAYLOAD_LEN:-64}"
IFACE="${IFACE:-bond0}"
QUEUE_MODE="${QUEUE_MODE:-all}"
QUEUE="${QUEUE:-0}"
LOCAL="${LOCAL:-213.239.141.12:52000}"
TARGET="${TARGET:-213.239.141.11:41000}"
RATE="${RATE:-1000}"
STATS_IFACES="${STATS_IFACES:-$IFACE}"
ETHTOOL="${ETHTOOL:-ethtool}"
PERF="${PERF:-perf}"
PERF_SUDO="${PERF_SUDO:-sudo}"
PERF_FREQ="${PERF_FREQ:-999}"
CALL_GRAPH="${CALL_GRAPH:-fp}"
PERF_DELAY_MS="${PERF_DELAY_MS:-}"
OUT_BASE="${OUT_BASE:-$ROOT/bench-results/analysis/$(date -u +%Y%m%dT%H%M%SZ)-xdp-$MODE}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "required command not found: $1" >&2
    exit 1
  fi
}
require_command "$PERF"

RUN_UID=$(id -u)
RUN_GID=$(id -g)
PERF_SUDO_ARGS=()
if [[ "$RUN_UID" != "0" ]]; then
  require_command "$PERF_SUDO"
  PERF_SUDO_ARGS=(-n)
  if ! "$PERF_SUDO" "${PERF_SUDO_ARGS[@]}" true >/dev/null 2>&1; then
    echo "$PERF_SUDO cannot run non-interactively; configure NOPASSWD or run as root" >&2
    exit 1
  fi
fi

mkdir -p "$OUT_BASE"

if [[ "${FORCE_FRAME_POINTERS:-1}" == "1" ]]; then
  if [[ -n "${RUSTFLAGS:-}" ]]; then
    if [[ "$RUSTFLAGS" != *force-frame-pointers* ]]; then
      export RUSTFLAGS="$RUSTFLAGS -C force-frame-pointers=yes"
    fi
  else
    export RUSTFLAGS="-C force-frame-pointers=yes"
  fi
fi

echo "building release XDP sender"
cargo build --release -p fast-socket-benchmarks --bin xdp-sender

SENDER_BIN="$ROOT/target/release/xdp-sender"
if [[ ! -x "$SENDER_BIN" ]]; then
  echo "expected benchmark binary was not built: $SENDER_BIN" >&2
  exit 1
fi

sender_args=(
  "$SENDER_BIN" "$MODE"
  --iface "$IFACE"
  --local "$LOCAL"
  --dest "$TARGET"
  --payload-len "$PAYLOAD_LEN"
  --duration-ms "$DURATION_MS"
)

if [[ "$MODE" == "blast" ]]; then
  if [[ "$QUEUE_MODE" == "all" ]]; then
    sender_args+=(--all-queues)
  else
    sender_args+=(--queue "$QUEUE")
  fi
else
  sender_args+=(--rate "$RATE")
fi

dump_nic_stats() {
  local phase="$1"
  local run_dir="$2"
  local out="$run_dir/nic-stats-$phase.txt"
  {
    echo "# captured_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    if [[ -z "$STATS_IFACES" ]]; then
      echo "# no interfaces configured"
    elif ! command -v "$ETHTOOL" >/dev/null 2>&1; then
      echo "# ethtool not found"
    else
      local iface
      for iface in $STATS_IFACES; do
        echo
        echo "## $iface"
        "$ETHTOOL" -S "$iface" || echo "# ethtool -S $iface failed"
      done
    fi
  } > "$out"
}

skipped() {
  local needle=",$1,"
  [[ ",${SKIP}," == *"$needle"* ]]
}

resolve_symbol_full_name() {
  # Resolve a substring to the full demangled symbol name as nm sees it.
  # Echoes the first match; nothing on no-match. Uses a temp file to avoid
  # the SIGPIPE-with-pipefail trap from awk exiting before nm finishes.
  local needle="$1"
  local nm_out
  nm_out="$(nm --demangle "$SENDER_BIN" 2>/dev/null || true)"
  awk -v n="$needle" '$2 ~ /^[TtW]$/ && index($0, n) > 0 { sub(/^[0-9a-f]+ . /, ""); print; exit }' <<<"$nm_out" || true
}

run_perf_mode() {
  # $1 = mode label (used for subdir)
  # $2 = log file label for sender
  # rest = perf record event/flags
  local label="$1"; shift
  local sublog="$1"; shift
  local sub="$OUT_BASE/$label"
  mkdir -p "$sub"

  dump_nic_stats before "$sub"

  local perf_args=(record -F "$PERF_FREQ" --call-graph "$CALL_GRAPH" -o "$sub/perf.data" "$@")
  if [[ -n "$PERF_DELAY_MS" ]]; then
    perf_args+=(-D "$PERF_DELAY_MS")
  fi
  perf_args+=(-- "${sender_args[@]}")

  echo "=== [$label] perf record $* ==="
  local rc=0
  if [[ "$RUN_UID" == "0" ]]; then
    set +e
    "$PERF" "${perf_args[@]}" > "$sub/$sublog" 2>&1
    rc=$?
    set -e
  else
    set +e
    "$PERF_SUDO" "${PERF_SUDO_ARGS[@]}" "$PERF" "${perf_args[@]}" > "$sub/$sublog" 2>&1
    rc=$?
    set -e
    [[ -f "$sub/perf.data" ]] && \
      "$PERF_SUDO" "${PERF_SUDO_ARGS[@]}" chown "$RUN_UID:$RUN_GID" "$sub/perf.data" || true
  fi

  dump_nic_stats after "$sub"

  if (( rc != 0 )); then
    echo "  ! perf record for [$label] exited with $rc (see $sub/$sublog)"
    return 0
  fi

  # Standard reports for this run.
  "$PERF" report -i "$sub/perf.data" --stdio --no-children > "$sub/report-self.txt" 2>/dev/null || true
  "$PERF" report -i "$sub/perf.data" --stdio > "$sub/report-children.txt" 2>/dev/null || true

  # Per-symbol annotation - shows per-instruction sample %.
  # perf annotate -s wants the FULL demangled symbol name; resolve via nm.
  if [[ -n "$ANNOTATE_SYMBOLS" ]]; then
    local sym fullsym
    mkdir -p "$sub/annotate"
    IFS=',' read -ra _syms <<<"$ANNOTATE_SYMBOLS"
    for sym in "${_syms[@]}"; do
      fullsym="$(resolve_symbol_full_name "::$sym")"
      [[ -z "$fullsym" ]] && fullsym="$(resolve_symbol_full_name "$sym")"
      if [[ -n "$fullsym" ]]; then
        "$PERF" annotate -i "$sub/perf.data" --stdio -s "$fullsym" \
          > "$sub/annotate/$sym.txt" 2>/dev/null || true
      fi
    done
  fi

  echo "wrote: $sub"
}

# Mode 1: precise cycles
if ! skipped cycles; then
  run_perf_mode cycles sender.log -e "cycles:pp"
fi

# Mode 2: L1d load misses (imprecise -- many AMD parts don't support :pp on this event)
if ! skipped l1miss; then
  run_perf_mode l1miss sender.log -e "L1-dcache-load-misses"
fi

# Mode 3: AMD IBS Op with ldlat filter (loads stalling >=50 cycles)
# Falls back gracefully if ibs_op isn't available.
if ! skipped ldlat; then
  if "$PERF" list 2>/dev/null | grep -q "ibs_op"; then
    run_perf_mode ldlat sender.log -d -e "ibs_op/ldlat=50,rand_en=1/p"
    # Sort load samples by stall latency (weight col) descending for easy reading.
    if [[ -f "$OUT_BASE/ldlat/perf.data" ]]; then
      "$PERF" script -i "$OUT_BASE/ldlat/perf.data" \
        -F ip,addr,weight,sym,dso,data_src 2>/dev/null \
        | sort -k3 -rn > "$OUT_BASE/ldlat/by-latency.txt" || true
    fi
  else
    echo "ibs_op event not available; skipping ldlat mode"
  fi
fi

# Mode 4: perf c2c for false-sharing / cross-core HITM detection
if ! skipped c2c; then
  sub="$OUT_BASE/c2c"
  mkdir -p "$sub"
  dump_nic_stats before "$sub"
  perf_args=(c2c record -F "$PERF_FREQ" --call-graph "$CALL_GRAPH" -o "$sub/perf.data")
  if [[ -n "$PERF_DELAY_MS" ]]; then
    perf_args+=(-D "$PERF_DELAY_MS")
  fi
  perf_args+=(-- "${sender_args[@]}")
  echo "=== [c2c] perf c2c record ==="
  if [[ "$RUN_UID" == "0" ]]; then
    "$PERF" "${perf_args[@]}" > "$sub/sender.log" 2>&1
  else
    "$PERF_SUDO" "${PERF_SUDO_ARGS[@]}" "$PERF" "${perf_args[@]}" > "$sub/sender.log" 2>&1
    "$PERF_SUDO" "${PERF_SUDO_ARGS[@]}" chown "$RUN_UID:$RUN_GID" "$sub/perf.data"
  fi
  dump_nic_stats after "$sub"

  "$PERF" c2c report --stdio -i "$sub/perf.data" \
    > "$sub/c2c-report.txt" 2>/dev/null || true
  "$PERF" c2c report --stdio -i "$sub/perf.data" --call-graph no \
    > "$sub/c2c-report-flat.txt" 2>/dev/null || true
  echo "wrote: $sub"
fi

cat > "$OUT_BASE/README.txt" <<EOF
Analysis run: $(date -u +%Y-%m-%dT%H:%M:%SZ)
Mode: $MODE
Duration per run: ${DURATION_MS}ms

Subdirectories:
  cycles/   - perf record -e cycles:pp
              - report-self.txt: function self time (precise)
              - annotate/<sym>.txt: per-instruction cycle %
  l1miss/   - perf record -e L1-dcache-load-misses:pp
              - same layout as cycles/, but samples are L1d misses
              - high % on a load => that load is missing L1
  ldlat/    - perf record -e ibs_op/ldlat=50/p -d
              - by-latency.txt: every sampled load with latency >= 50 cyc,
                sorted descending. Columns: ip, addr, weight (cycles),
                sym, dso, data_src (L1/L2/L3/LFB/HITM etc.)
              - cross-reference (ip) with the asm in cycles/annotate/ to
                find the exact instruction
  c2c/      - perf c2c record / report (cache-to-cache contention)
              - c2c-report.txt: cache lines ranked by HITM rate
              - c2c-report-flat.txt: same without call-graph
              - lines with HITM stores => true or false sharing

Tip: open report-self.txt in cycles/ first to identify hot functions, then
look at the matching cycles/annotate/<fn>.txt and l1miss/annotate/<fn>.txt
side by side to see which instructions are cycle-hungry and which are
cache-cold.
EOF

echo
echo "results: $OUT_BASE"
echo "open $OUT_BASE/README.txt for guidance"
