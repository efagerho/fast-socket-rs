#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: benchmarks/profile-xdp.sh

Profiles the XDP UDP sender (blast mode) under perf.

Defaults:
  PROFILE_SECONDS=10     derived sender duration when DURATION_MS is unset
  DURATION_MS=           sender duration in milliseconds
  PAYLOAD_LEN=64         UDP payload size
  IFACE=bond0            interface passed to xdp-sender
  THREADS=4              worker threads; all NIC queues split into THREADS blocks
  LOCAL=213.239.141.12:52000
  TARGET=213.239.141.11:41000
  STATS_IFACES=$IFACE    interfaces captured with ethtool -S before/after
  ETHTOOL=ethtool        command used for NIC stats snapshots
  OUT_DIR=bench-results/profiles/<timestamp>-xdp-<mode>
  PERF_FREQ=999          perf sample frequency
  CALL_GRAPH=fp          perf call graph mode
  PERF_SUDO=sudo         command used to run perf record as root
  PERF_DELAY_MS=         optional perf record delay in milliseconds

Flamegraph conversion uses one of:
  inferno-collapse-perf + inferno-flamegraph
  stackcollapse-perf.pl + flamegraph.pl

If neither is installed, the script still writes perf.data, perf.script, and
perf report text files.
EOF
}

MODE=blast
while [[ $# -gt 0 ]]; do
  case "$1" in
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

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE_SECONDS="${PROFILE_SECONDS:-10}"
DURATION_MS="${DURATION_MS:-$((PROFILE_SECONDS * 1000))}"
PAYLOAD_LEN="${PAYLOAD_LEN:-64}"
IFACE="${IFACE:-bond0}"
THREADS="${THREADS:-4}"
LOCAL="${LOCAL:-213.239.141.12:52000}"
TARGET="${TARGET:-213.239.141.11:41000}"
STATS_IFACES="${STATS_IFACES:-$IFACE}"
ETHTOOL="${ETHTOOL:-ethtool}"
OUT_DIR="${OUT_DIR:-"$ROOT/bench-results/profiles/$(date -u +%Y%m%dT%H%M%SZ)-xdp-$MODE"}"
PERF="${PERF:-perf}"
PERF_SUDO="${PERF_SUDO:-sudo}"
PERF_SUDO_ARGS=()
PERF_FREQ="${PERF_FREQ:-999}"
CALL_GRAPH="${CALL_GRAPH:-fp}"
PERF_DELAY_MS="${PERF_DELAY_MS:-}"
RUN_UID="$(id -u)"
RUN_GID="$(id -g)"

SENDER_BIN="$ROOT/target/release/xdp-sender"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "required command not found: $1" >&2
    exit 1
  fi
}

require_command cargo
require_command "$PERF"
if [[ "$RUN_UID" != "0" ]]; then
  require_command "$PERF_SUDO"
  PERF_SUDO_ARGS=(-n)
  if ! "$PERF_SUDO" "${PERF_SUDO_ARGS[@]}" true >/dev/null 2>&1; then
    echo "$PERF_SUDO cannot run non-interactively; configure NOPASSWD or run as root" >&2
    exit 1
  fi
fi

mkdir -p "$OUT_DIR"

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

if [[ ! -x "$SENDER_BIN" ]]; then
  echo "expected benchmark binary was not built: $SENDER_BIN" >&2
  exit 1
fi

write_run_env() {
  {
    echo "target=$TARGET"
    echo "mode=$MODE"
    echo "iface=$IFACE"
    echo "local=$LOCAL"
    echo "payload_len=$PAYLOAD_LEN"
    echo "duration_ms=$DURATION_MS"
    echo "threads=$THREADS"
    echo "perf_freq=$PERF_FREQ"
    echo "call_graph=$CALL_GRAPH"
    echo "stats_ifaces=$STATS_IFACES"
    if [[ -n "$PERF_DELAY_MS" ]]; then
      echo "perf_delay_ms=$PERF_DELAY_MS"
    fi
  } > "$OUT_DIR/run.env"
}

dump_nic_stats() {
  local phase="$1"
  local run_dir="$2"
  local out="$run_dir/nic-stats-$phase.txt"

  {
    echo "# captured_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "# command: $ETHTOOL -S <iface>"

    if [[ -z "$STATS_IFACES" ]]; then
      echo "# no interfaces configured in STATS_IFACES"
    elif ! command -v "$ETHTOOL" >/dev/null 2>&1; then
      echo "# ethtool command not found: $ETHTOOL"
    else
      local iface
      for iface in $STATS_IFACES; do
        echo
        echo "## $iface"
        if "$ETHTOOL" -S "$iface"; then
          :
        else
          local status=$?
          echo "# ethtool -S $iface failed with exit status $status"
        fi
      done
    fi
  } > "$out"
}

record_sender_profile() {
  local perf_data="$1"
  local log="$2"
  shift 2

  local perf_args=(
    record
    -F "$PERF_FREQ"
    --call-graph "$CALL_GRAPH"
    -o "$perf_data"
  )
  if [[ -n "$PERF_DELAY_MS" ]]; then
    perf_args+=(-D "$PERF_DELAY_MS")
  fi
  perf_args+=(-- "$@")

  if [[ "$RUN_UID" == "0" ]]; then
    "$PERF" "${perf_args[@]}" > "$log" 2>&1
    return
  fi

  "$PERF_SUDO" "${PERF_SUDO_ARGS[@]}" "$PERF" "${perf_args[@]}" > "$log" 2>&1
  "$PERF_SUDO" "${PERF_SUDO_ARGS[@]}" chown "$RUN_UID:$RUN_GID" "$perf_data"
}

write_reports() {
  local run_dir="$1"
  local perf_data="$run_dir/perf.data"

  "$PERF" report -i "$perf_data" --stdio > "$run_dir/report-children.txt" 2>/dev/null || true
  "$PERF" report -i "$perf_data" --stdio --no-children > "$run_dir/report-self.txt" 2>/dev/null || true
  "$PERF" report -i "$perf_data" --stdio --sort dso > "$run_dir/report-dso.txt" 2>/dev/null || true
}

convert_flamegraph() {
  local run_dir="$1"
  local perf_data="$run_dir/perf.data"
  local perf_script="$run_dir/perf.script"
  local collapsed="$run_dir/stacks.folded"
  local svg="$run_dir/flamegraph.svg"

  "$PERF" script -i "$perf_data" > "$perf_script"

  if command -v inferno-collapse-perf >/dev/null 2>&1 &&
     command -v inferno-flamegraph >/dev/null 2>&1; then
    inferno-collapse-perf "$perf_script" > "$collapsed"
    inferno-flamegraph "$collapsed" > "$svg"
    echo "wrote $svg"
    return
  fi

  if command -v stackcollapse-perf.pl >/dev/null 2>&1 &&
     command -v flamegraph.pl >/dev/null 2>&1; then
    stackcollapse-perf.pl "$perf_script" > "$collapsed"
    flamegraph.pl "$collapsed" > "$svg"
    echo "wrote $svg"
    return
  fi

  cat > "$run_dir/FLAMEGRAPH_MISSING.txt" <<'EOF'
Flamegraph tools were not found.

Install one of:
  cargo install inferno
  https://github.com/brendangregg/FlameGraph on PATH

Then rerun conversion manually, for example:
  perf script -i perf.data > perf.script
  inferno-collapse-perf perf.script > stacks.folded
  inferno-flamegraph stacks.folded > flamegraph.svg
EOF
  echo "flamegraph tools missing; wrote $perf_script and $run_dir/FLAMEGRAPH_MISSING.txt"
}

sender_args=(
  "$SENDER_BIN" "$MODE"
  --iface "$IFACE"
  --local "$LOCAL"
  --dest "$TARGET"
  --payload-len "$PAYLOAD_LEN"
  --duration-ms "$DURATION_MS"
  --threads "$THREADS"
)

write_run_env

echo "profiling XDP sender: ${sender_args[*]}"
dump_nic_stats before "$OUT_DIR"
record_sender_profile "$OUT_DIR/perf.data" "$OUT_DIR/sender.log" "${sender_args[@]}"
dump_nic_stats after "$OUT_DIR"
write_reports "$OUT_DIR"
convert_flamegraph "$OUT_DIR"

echo "results: $OUT_DIR"
