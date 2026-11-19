CREATE TABLE IF NOT EXISTS items (
    id    UInt64,
    name  String,
    value String
) ENGINE = MergeTree() ORDER BY id;
