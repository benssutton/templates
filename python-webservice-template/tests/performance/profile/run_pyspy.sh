#!/usr/bin/env bash
# Attach py-spy to the running app container and capture profiles for Layer 2
# contention classification (CPU/GIL vs I/O). Run a k6 profile concurrently so
# the samples land under load.
#
# Usage: tests/performance/profile/run_pyspy.sh [DURATION_SECONDS]
# Requires the profiling stack (docker-compose.profiling.yml) so the app
# container has the SYS_PTRACE capability.
set -euo pipefail

DURATION="${1:-30}"
OUT_DIR="$(cd "$(dirname "$0")" && pwd)/artifacts"
mkdir -p "$OUT_DIR"

APP_CID="$(docker compose ps -q app)"
if [ -z "$APP_CID" ]; then
  echo "app container not running. Start the profiling stack first:" >&2
  echo "  docker compose -f docker-compose.yml -f docker-compose.profiling.yml up -d --build" >&2
  exit 1
fi

# py-spy ships manylinux wheels, so pip install needs no compiler.
docker exec "$APP_CID" pip install --quiet py-spy

# The app is the container's main process; fall back to PID 1.
APP_PID="$(docker exec "$APP_CID" sh -c "pgrep -f 'main.py|uvicorn|main:app' | head -n1 || echo 1")"
echo "Profiling app pid=$APP_PID for ${DURATION}s..."

# (1) Full flamegraph including threads parked in I/O (--idle): shows where wall
#     time goes overall.
docker exec "$APP_CID" py-spy record --pid "$APP_PID" --duration "$DURATION" \
  --idle --format flamegraph --output /tmp/flame_all.svg
docker cp "$APP_CID:/tmp/flame_all.svg" "$OUT_DIR/flame_all.svg"

# (2) GIL-only flamegraph (--gil): samples only threads holding the GIL. Heavy
#     mass here on Python frames => CPU/GIL-bound; sparse => I/O-bound.
docker exec "$APP_CID" py-spy record --pid "$APP_PID" --duration "$DURATION" \
  --gil --format flamegraph --output /tmp/flame_gil.svg
docker cp "$APP_CID:/tmp/flame_gil.svg" "$OUT_DIR/flame_gil.svg"

# (3) One-shot thread dump: shows what each thread is doing right now (e.g.
#     socket recv vs Python CPU frame).
docker exec "$APP_CID" py-spy dump --pid "$APP_PID" > "$OUT_DIR/pyspy-dump.txt" || true

echo "Artifacts written to $OUT_DIR:"
echo "  flame_all.svg   — overall wall-time flamegraph"
echo "  flame_gil.svg   — GIL-held flamegraph (CPU/GIL signal)"
echo "  pyspy-dump.txt  — instantaneous thread states"
echo "Interpret with docs/superpowers/perf-profiling-runbook.md."
