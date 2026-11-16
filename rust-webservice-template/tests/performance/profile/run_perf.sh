#!/usr/bin/env bash
# Layer 2 (PERF_PROFILING_RUNBOOK.md): sample the running app with Linux perf
# and render an on-CPU flamegraph with inferno. This is the perf -> inferno
# pipeline `cargo flamegraph` runs, split between the container (perf) and the
# host (inferno), because the runtime image has no cargo. Run a k6 profile
# concurrently so the samples land under load.
#
# Usage: tests/performance/profile/run_perf.sh [DURATION_SECONDS]   (default 30)
# perf stat and perf record run one after the other: total runtime is ~2 x DURATION.
# Needs the profiling stack (docker-compose.profiling.yml) and inferno on the host:
#   cargo install inferno --locked --version 0.12.8
set -euo pipefail

DURATION="${1:-30}"
OUT_DIR="$(cd "$(dirname "$0")" && pwd)/artifacts"
mkdir -p "$OUT_DIR"

# Resolve the service root from this script's location so `docker compose`
# finds the base compose file regardless of the caller's working directory.
cd "$(dirname "$0")/../../.."

for tool in inferno-collapse-perf inferno-flamegraph; do
  if ! command -v "$tool" >/dev/null; then
    echo "$tool not found. Install it with: cargo install inferno --locked --version 0.12.8" >&2
    exit 1
  fi
done

APP_CID="$(docker compose ps -q app)"
if [ -z "$APP_CID" ]; then
  echo "app container not running. Start the profiling stack first:" >&2
  echo "  docker compose -f docker-compose.yml -f docker-compose.profiling.yml up -d --build --wait" >&2
  exit 1
fi

# MSYS_NO_PATHCONV=1 stops Git Bash rewriting container-side paths such as
# /tmp/perf.data into Windows paths. Harmless on Linux/WSL. The `docker cp`
# line is left without it so its host-side destination is still converted.

# perf is installed as root into the running container. The install is
# ephemeral: re-run this script after a `docker compose restart`. Debian
# trixie's linux-perf ships /usr/bin/perf; its userspace works on newer kernels.
MSYS_NO_PATHCONV=1 docker exec -u 0 "$APP_CID" sh -c '
  command -v perf >/dev/null || {
    apt-get update -qq &&
    apt-get install -y -qq --no-install-recommends linux-perf >/dev/null; }'

# The app is PID 1 (exec-form CMD); scan /proc anyway (no procps in the image).
APP_PID="$(MSYS_NO_PATHCONV=1 docker exec "$APP_CID" sh -c '
  for f in /proc/[0-9]*/cmdline; do
    if [ "$(tr "\0" " " < "$f" 2>/dev/null)" = "/app/rust-template " ]; then
      p="${f%/cmdline}"; echo "${p#/proc/}"; exit 0
    fi
  done
  echo 1')"
echo "Profiling app pid=$APP_PID for ${DURATION}s (perf stat), then ${DURATION}s (perf record)..."

# (1) CPU utilisation over the window: "CPUs utilized" near 0 means the app
#     is mostly waiting (I/O-bound); near the worker-thread count means it is
#     computing (CPU-bound). perf stat reports on stderr.
MSYS_NO_PATHCONV=1 docker exec -u 0 "$APP_CID" \
  perf stat -e task-clock,context-switches,cpu-migrations -p "$APP_PID" -- sleep "$DURATION" \
  2> "$OUT_DIR/perf-stat.txt"

# (2) On-CPU samples at 99 Hz with frame-pointer call graphs, rendered as a
#     flamegraph on the host.
MSYS_NO_PATHCONV=1 docker exec -u 0 "$APP_CID" \
  perf record -F 99 --call-graph fp -p "$APP_PID" -o /tmp/perf.data -- sleep "$DURATION"
MSYS_NO_PATHCONV=1 docker exec -u 0 "$APP_CID" \
  sh -c 'perf script -i /tmp/perf.data > /tmp/perf.script 2>/dev/null'
docker cp "$APP_CID:/tmp/perf.script" "$OUT_DIR/perf.script"
inferno-collapse-perf "$OUT_DIR/perf.script" > "$OUT_DIR/perf.folded"
inferno-flamegraph --title "rust-template on-CPU, ${DURATION}s" \
  "$OUT_DIR/perf.folded" > "$OUT_DIR/flame_cpu.svg"

echo "Artifacts written to $OUT_DIR:"
echo "  flame_cpu.svg   - on-CPU flamegraph (where compute time goes)"
echo "  perf-stat.txt   - task-clock / CPUs utilized (CPU- vs I/O-bound signal)"
echo "  perf.folded     - collapsed stacks (diffable with inferno-diff-folded)"
grep -E "task-clock|CPUs utilized" "$OUT_DIR/perf-stat.txt" || true
echo "Interpret with PERF_PROFILING_RUNBOOK.md (service root)."
