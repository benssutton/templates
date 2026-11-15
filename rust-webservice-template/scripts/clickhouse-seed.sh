#!/bin/bash
# Compose-only: seed default.items from the same Arrow file the tests use.
# Runs once, from /docker-entrypoint-initdb.d, on an empty data directory.
set -e
clickhouse-client --password "${CLICKHOUSE_PASSWORD:-}" \
  --query="INSERT INTO default.items FORMAT Arrow" < /tmp/clickhouse_seed_data.ipc
