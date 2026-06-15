#!/usr/bin/env bash
# Attach py-spy to the running app container and capture profiles for Layer 2
# contention classification (CPU/GIL vs I/O). Run a k6 profile concurrently so
# the samples land under load.
#
# Usage: tests/performance/profile/run_pyspy.sh [DURATION_SECONDS]
# The two py-spy record passes run sequentially, so total runtime is ~2 x DURATION.
# Requires the profiling stack (docker-compose.profiling.yml) so the app
# container has the SYS_PTRACE capability.
set -euo pipefail

DURATION="${1:-30}"
OUT_DIR="$(cd "$(dirname "$0")" && pwd)/artifacts"
mkdir -p "$OUT_DIR"

# Resolve the repo root from this script's location so `docker compose` finds the
# base compose file regardless of the caller's working directory. OUT_DIR was made
# absolute above, so this cd does not affect where artifacts are written.
cd "$(dirname "$0")/../../.."

APP_CID="$(docker compose ps -q app)"
if [ -z "$APP_CID" ]; then
  echo "app container not running. Start the profiling stack first:" >&2
  echo "  docker compose -f docker-compose.yml -f docker-compose.profiling.yml up -d --build" >&2
  exit 1
fi

# py-spy ships manylinux wheels, so pip install needs no compiler. This install is
# ephemeral (lost on container restart) — re-run this whole script, not just py-spy,
# after a `docker compose restart`.
docker exec "$APP_CID" pip install --quiet py-spy

# python:3.12-slim has no procps (pgrep absent). Fall back to a /proc scan which
# works in any Linux container without extra packages.
APP_PID="$(docker exec "$APP_CID" sh -c '
  pgrep -fo "uvicorn|main:app|main.py" 2>/dev/null && exit 0
  for f in /proc/[0-9]*/cmdline; do
    pid="${f%/cmdline}"; pid="${pid#/proc/}"
    cmd=$(tr "\0" " " < "$f" 2>/dev/null)
    case "$cmd" in *uvicorn*|*main.py*) echo "$pid"; break;; esac
  done
' || true)"
APP_PID="${APP_PID:-1}"
echo "Profiling app pid=$APP_PID for ${DURATION}s..."

# MSYS_NO_PATHCONV=1 on the `docker exec` lines stops Git Bash from rewriting the
# container-side `/tmp/...` argument into a Windows path (e.g. C:/Users/.../Temp/...)
# before docker sees it — that mangling is why py-spy reported "No such file or
# directory" after sampling. It is harmless on Linux/WSL. The `docker cp` lines are
# deliberately left without the prefix so their host-side destination path is still
# converted normally.

# (1) Full flamegraph including threads parked in I/O (--idle): shows where wall
#     time goes overall.
MSYS_NO_PATHCONV=1 docker exec "$APP_CID" py-spy record --pid "$APP_PID" --duration "$DURATION" \
  --idle --format flamegraph --output /tmp/flame_all.svg
docker cp "$APP_CID:/tmp/flame_all.svg" "$OUT_DIR/flame_all.svg"

# (2) GIL-only flamegraph (--gil): samples only threads holding the GIL. Heavy
#     mass here on Python frames => CPU/GIL-bound; sparse => I/O-bound.
#     When the app holds the GIL rarely (a strongly I/O-bound workload, or light
#     load), py-spy collects no GIL samples and exits non-zero with "No stack
#     counts found". That is a *result*, not a failure — an empty GIL flamegraph
#     is itself the I/O-bound signal — so we keep going (the `if` also shields it
#     from `set -e`) and still capture the thread dump below.
if MSYS_NO_PATHCONV=1 docker exec "$APP_CID" py-spy record --pid "$APP_PID" --duration "$DURATION" \
    --gil --format flamegraph --output /tmp/flame_gil.svg; then
  docker cp "$APP_CID:/tmp/flame_gil.svg" "$OUT_DIR/flame_gil.svg"
else
  echo "NOTE: no GIL-held samples captured — the app rarely held the GIL during" >&2
  echo "      sampling, which is itself a strong I/O-bound signal (see the" >&2
  echo "      decision rule in PERF_PROFILING_RUNBOOK.md). Skipping flame_gil.svg." >&2
fi

# (3) One-shot thread dump: shows what each thread is doing right now (e.g.
#     socket recv vs Python CPU frame).
docker exec "$APP_CID" py-spy dump --pid "$APP_PID" > "$OUT_DIR/pyspy-dump.txt" || true

echo "Artifacts written to $OUT_DIR:"
echo "  flame_all.svg   — overall wall-time flamegraph"
echo "  flame_gil.svg   — GIL-held flamegraph (CPU/GIL signal)"
echo "  pyspy-dump.txt  — instantaneous thread states"
echo "Interpret with PERF_PROFILING_RUNBOOK.md (template root)."
