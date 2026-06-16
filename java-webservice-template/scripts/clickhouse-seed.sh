#!/bin/bash
set -e
clickhouse-client --password "${CLICKHOUSE_PASSWORD:-}" --query="INSERT INTO default.items VALUES (1,'seed-a','x'),(2,'seed-b','y'),(3,'seed-c','z')"
