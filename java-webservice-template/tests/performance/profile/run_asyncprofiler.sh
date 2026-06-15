#!/usr/bin/env bash
# Attach async-profiler to the running app container and capture a wall-clock and a
# CPU flamegraph (the py-spy --idle/--gil analogue). Requires the profiling overlay
# (docker-compose.profiling.yml) for SYS_ADMIN. Run a k6 profile under load in
# another shell so samples land while the app is busy.
#
# Usage: tests/performance/profile/run_asyncprofiler.sh [DURATION_SECONDS]
# On Windows Git Bash, MSYS_NO_PATHCONV=1 stops container paths being mangled.
set -euo pipefail
DURATION="${1:-30}"
OUT_DIR="$(cd "$(dirname "$0")" && pwd)/artifacts"
mkdir -p "$OUT_DIR"
cd "$(dirname "$0")/../../.."

APP_CID="$(docker compose ps -q app)"
if [ -z "$APP_CID" ]; then
  echo "app container not running. Start the profiling stack first:" >&2
  echo "  docker compose -f docker-compose.yml -f docker-compose.profiling.yml up -d --build" >&2
  exit 1
fi

# Install async-profiler into the container (ephemeral).
MSYS_NO_PATHCONV=1 docker exec "$APP_CID" sh -c '
  test -d /opt/async-profiler || (
    apt-get update -qq && apt-get install -y -qq curl >/dev/null &&
    curl -sL https://github.com/async-profiler/async-profiler/releases/latest/download/async-profiler-linux-x64.tar.gz | tar xz -C /opt &&
    mv /opt/async-profiler-* /opt/async-profiler )'

APP_PID="$(MSYS_NO_PATHCONV=1 docker exec "$APP_CID" sh -c "pgrep -f app.jar | head -1")"
echo "Profiling app pid=$APP_PID for ${DURATION}s..."

# (1) Wall-clock flamegraph (overall time incl. parked threads).
MSYS_NO_PATHCONV=1 docker exec "$APP_CID" /opt/async-profiler/bin/asprof \
  -e wall -d "$DURATION" -f /tmp/flame_wall.html "$APP_PID"
docker cp "$APP_CID:/tmp/flame_wall.html" "$OUT_DIR/flame_wall.html"

# (2) CPU flamegraph (on-CPU only). Sparse here => I/O-bound (see the decision rule).
MSYS_NO_PATHCONV=1 docker exec "$APP_CID" /opt/async-profiler/bin/asprof \
  -e cpu -d "$DURATION" -f /tmp/flame_cpu.html "$APP_PID"
docker cp "$APP_CID:/tmp/flame_cpu.html" "$OUT_DIR/flame_cpu.html"

echo "Artifacts written to $OUT_DIR: flame_wall.html, flame_cpu.html"
