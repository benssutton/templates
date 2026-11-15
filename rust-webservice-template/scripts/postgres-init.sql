-- pgvector: available because the database runs the pgvector/pgvector image.
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS configuration (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
