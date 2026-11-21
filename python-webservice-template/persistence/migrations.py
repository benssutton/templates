"""Versioned SQL migrations for Postgres and ClickHouse, applied at startup.

Files live in migrations/<store>/NNN_description.sql and are applied in version
order; each store records what it has applied in a `schema_migrations` table.
Never edit an applied file: add the next number instead.

Postgres: every pending file runs in ONE transaction under a transaction-scoped
advisory lock, so replicas starting together apply each version exactly once and
a failed migration leaves nothing behind.

ClickHouse has neither transactions nor advisory locks, so its files must be
idempotent (`IF NOT EXISTS`) and hold a single statement each: two replicas may
both apply a version, and the second run must be a no-op.
"""

import re
from dataclasses import dataclass
from pathlib import Path

import asyncpg
from clickhouse_connect.driver.asyncclient import AsyncClient

MIGRATIONS_DIR = Path(__file__).parents[1] / "migrations"

# Advisory-lock key serialising Postgres migrations across connections.
SCHEMA_LOCK_ID = 0x7273_5F73_6368_656D

_FILE_NAME = re.compile(r"(\d+)_(\w+)\.sql")


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    sql: str


def load_migrations(store: str) -> list[Migration]:
    migrations = []
    for path in (MIGRATIONS_DIR / store).glob("*.sql"):
        match = _FILE_NAME.fullmatch(path.name)
        if match is None:
            raise ValueError(f"migration file name must be NNN_name.sql: {path}")
        migrations.append(
            Migration(int(match[1]), match[2], path.read_text(encoding="utf-8"))
        )
    migrations.sort(key=lambda m: m.version)
    versions = [m.version for m in migrations]
    if len(set(versions)) != len(versions):
        raise ValueError(f"duplicate {store} migration version in {versions}")
    return migrations


async def migrate_postgres(pool: asyncpg.Pool) -> list[int]:
    """Apply pending Postgres migrations; return the versions applied."""
    applied_now = []
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute("SELECT pg_advisory_xact_lock($1)", SCHEMA_LOCK_ID)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version    INTEGER PRIMARY KEY,
                name       TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """)
        applied = {
            row["version"]
            for row in await conn.fetch("SELECT version FROM schema_migrations")
        }
        for migration in load_migrations("postgres"):
            if migration.version in applied:
                continue
            await conn.execute(migration.sql)
            await conn.execute(
                "INSERT INTO schema_migrations (version, name) VALUES ($1, $2)",
                migration.version,
                migration.name,
            )
            applied_now.append(migration.version)
    return applied_now


async def migrate_clickhouse(client: AsyncClient) -> list[int]:
    """Apply pending ClickHouse migrations; return the versions applied."""
    await client.command("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version    UInt32,
            name       String,
            applied_at DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree ORDER BY version
        """)
    result = await client.query("SELECT version FROM schema_migrations FINAL")
    applied = {row[0] for row in result.result_rows}
    applied_now = []
    for migration in load_migrations("clickhouse"):
        if migration.version in applied:
            continue
        await client.command(migration.sql.strip().rstrip(";"))
        await client.insert(
            "schema_migrations",
            [[migration.version, migration.name]],
            column_names=["version", "name"],
        )
        applied_now.append(migration.version)
    return applied_now
