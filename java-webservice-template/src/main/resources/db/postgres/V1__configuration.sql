-- pgvector (the pgvector/pgvector image): enabled deliberately as the foundation for
-- downstream embedding / similarity-search features; no template table uses it yet.
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS configuration (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
