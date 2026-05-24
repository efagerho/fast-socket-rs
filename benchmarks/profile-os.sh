#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: benchmarks/profile-os.sh [--mode all|blast|ping]

Profiles the OS UDP benchmark listener while a matching sender drives load.

Defaults:
  --mode all              run blast/count, then ping/pong
  PROFILE_SECONDS=10     perf recording duration per mode
  WARMUP_MS=2000         warmup sender duration before perf attaches
  PAYLOAD_LEN=64         UDP payload size
  HOST=127.0.0.1         loopback address
  PORT_BASE=41000        blast uses PORT_BASE, ping uses PORT_BASE+1
  SENDER_THREADS=8       sender worker threads
  LISTENER_CPU=          optional CPU for os-listener --cpu
  STATS_IFACES=          interfaces captured with ethtool -S before/after;
                         defaults to IFACE or the route device for HOST
  ETHTOOL=ethtool        command used for NIC stats snapshots
  OUT_DIR=bench-results/profiles/<timestamp>
  PERF_FREQ=999          perf sample frequency
  CALL_GRAPH=fp          perf call graph mode
  PERF_SUDO=sudo         command used to run perf record as root

Flamegraph conversion uses one of:
  inferno-collapse-perf + inferno-flamegraph
  stackcollapse-perf.pl + flamegraph.pl

If neither is installed, the script still writes perf.data and perf.script.
EOF
}

MODE=all
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      if [[ $# -lt 2 ]]; then
        echo "--mode requires a value: all, blast, or ping" >&2
        exit 2
      fi
      MODE="${2:-}"
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
  all|blast|ping) ;;
  *)
    echo "--mode must be one of: all, blast, ping" >&2
    exit 2
    ;;
esac

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE_SECONDS="${PROFILE_SECONDS:-10}"
WARMUP_MS="${WARMUP_MS:-2000}"
PAYLOAD_LEN="${PAYLOAD_LEN:-64}"
HOST="${HOST:-127.0.0.1}"
PORT_BASE="${PORT_BASE:-41000}"
SENDER_THREADS="${SENDER_THREADS:-8}"
LISTENER_CPU="${LISTENER_CPU:-}"
STATS_IFACES="${STATS_IFACES:-${IFACE:-}}"
ETHTOOL="${ETHTOOL:-ethtool}"
OUT_DIR="${OUT_DIR:-"$ROOT/bench-results/profiles/$(date -u +%Y%m%dT%H%M%SZ)"}"
PERF="${PERF:-perf}"
PERF_SUDO="${PERF_SUDO:-sudo}"
PERF_SUDO_ARGS=()
PERF_FREQ="${PERF_FREQ:-999}"
CALL_GRAPH="${CALL_GRAPH:-fp}"
SENDER_EXTRA_MS="${SENDER_EXTRA_MS:-1500}"
RUN_UID="$(id -u)"
RUN_GID="$(id -g)"

LISTENER_BIN="$ROOT/target/release/os-listener"
SENDER_BIN="$ROOT/target/release/os-sender"

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

resolve_stats_ifaces() {
  if [[ -n "$STATS_IFACES" ]] || ! command -v ip >/dev/null 2>&1; then
    return
  fi

  local route
  route="$(ip -o route get "$HOST" 2>/dev/null || true)"
  STATS_IFACES="$(sed -n 's/.* dev \([^ ]*\).*/\1/p' <<<"$route" | head -n1)"
}

resolve_stats_ifaces
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

echo "building release benchmark binaries"
cargo build --release -p fast-socket-benchmarks --bins

if [[ ! -x "$LISTENER_BIN" || ! -x "$SENDER_BIN" ]]; then
  echo "expected benchmark binaries were not built:" >&2
  echo "  $LISTENER_BIN" >&2
  echo "  $SENDER_BIN" >&2
  exit 1
fi

listener_pid=""
load_pid=""

cleanup() {
  if [[ -n "${load_pid:-}" ]] && kill -0 "$load_pid" 2>/dev/null; then
    kill "$load_pid" 2>/dev/null || true
    wait "$load_pid" 2>/dev/null || true
  fi
  if [[ -n "${listener_pid:-}" ]] && kill -0 "$listener_pid" 2>/dev/null; then
    kill "$listener_pid" 2>/dev/null || true
    wait "$listener_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

record_listener_profile() {
  local pid="$1"
  local perf_data="$2"

  if [[ "$RUN_UID" == "0" ]]; then
    "$PERF" record \
      -F "$PERF_FREQ" \
      --call-graph "$CALL_GRAPH" \
      -p "$pid" \
      -o "$perf_data" \
      -- sleep "$PROFILE_SECONDS"
    return
  fi

  "$PERF_SUDO" "${PERF_SUDO_ARGS[@]}" "$PERF" record \
    -F "$PERF_FREQ" \
    --call-graph "$CALL_GRAPH" \
    -p "$pid" \
    -o "$perf_data" \
    -- sleep "$PROFILE_SECONDS"

  "$PERF_SUDO" "${PERF_SUDO_ARGS[@]}" chown "$RUN_UID:$RUN_GID" "$perf_data"
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

run_sender_foreground() {
  local mode="$1"
  local dest="$2"
  local duration_ms="$3"
  local log="$4"

  "$SENDER_BIN" "$mode" \
    --dest "$dest" \
    --payload-len "$PAYLOAD_LEN" \
    --threads "$SENDER_THREADS" \
    --duration-ms "$duration_ms" \
    --timeout-ms 1000 \
    >"$log" 2>&1
}

run_sender_background() {
  local mode="$1"
  local dest="$2"
  local duration_ms="$3"
  local log="$4"

  "$SENDER_BIN" "$mode" \
    --dest "$dest" \
    --payload-len "$PAYLOAD_LEN" \
    --threads "$SENDER_THREADS" \
    --duration-ms "$duration_ms" \
    --timeout-ms 1000 \
    >"$log" 2>&1 &
  load_pid=$!
}

profile_one() {
  local name="$1"
  local listener_mode="$2"
  local sender_mode="$3"
  local port="$4"
  local bind="$HOST:$port"
  local run_dir="$OUT_DIR/$name"
  local listener_duration_ms=$((WARMUP_MS + PROFILE_SECONDS * 1000 + SENDER_EXTRA_MS + 30000))
  local sender_profile_ms=$((PROFILE_SECONDS * 1000 + SENDER_EXTRA_MS))

  mkdir -p "$run_dir"
  echo "== $name =="
  echo "starting listener: $listener_mode on $bind"

  local listener_args=(
    "$LISTENER_BIN" "$listener_mode"
    --bind "$bind"
    --duration-ms "$listener_duration_ms"
  )
  if [[ -n "$LISTENER_CPU" ]]; then
    listener_args+=(--cpu "$LISTENER_CPU")
    echo "pinning listener and setting SO_INCOMING_CPU to CPU $LISTENER_CPU"
  fi

  "${listener_args[@]}" >"$run_dir/listener.log" 2>&1 &
  listener_pid=$!

  sleep 0.25
  if ! kill -0 "$listener_pid" 2>/dev/null; then
    echo "listener failed to start; log follows:" >&2
    cat "$run_dir/listener.log" >&2 || true
    exit 1
  fi

  echo "warming listener for ${WARMUP_MS}ms"
  dump_nic_stats before "$run_dir"
  run_sender_foreground "$sender_mode" "$bind" "$WARMUP_MS" "$run_dir/warmup-sender.log"

  echo "recording listener pid $listener_pid for ${PROFILE_SECONDS}s"
  run_sender_background "$sender_mode" "$bind" "$sender_profile_ms" "$run_dir/profile-sender.log"
  record_listener_profile "$listener_pid" "$run_dir/perf.data"

  wait "$load_pid" || {
    echo "profile sender failed; log follows:" >&2
    cat "$run_dir/profile-sender.log" >&2 || true
    exit 1
  }
  load_pid=""

  kill "$listener_pid" 2>/dev/null || true
  wait "$listener_pid" 2>/dev/null || true
  listener_pid=""

  dump_nic_stats after "$run_dir"
  convert_flamegraph "$run_dir"
}

case "$MODE" in
  all)
    profile_one "blast" "count" "blast" "$PORT_BASE"
    profile_one "ping" "pong" "ping" "$((PORT_BASE + 1))"
    ;;
  blast)
    profile_one "blast" "count" "blast" "$PORT_BASE"
    ;;
  ping)
    profile_one "ping" "pong" "ping" "$((PORT_BASE + 1))"
    ;;
esac

echo "results: $OUT_DIR"
