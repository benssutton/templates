-- Unqualified table name: created in the database the client connects to
-- (CLICKHOUSE_DATABASE). One idempotent statement per file (see persistence/migrations.py).
CREATE TABLE IF NOT EXISTS items (
    id    UInt64,
    name  String,
    value String
) ENGINE = MergeTree() ORDER BY id;
