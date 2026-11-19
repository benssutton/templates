import asyncio
import uuid

import asyncpg
from pydantic import SecretStr

from persistence.analytics_store.clickhouse.clickhouse_client import ClickHouseClient
from persistence.migrations import (
    load_migrations,
    migrate_clickhouse,
    migrate_postgres,
)
from settings import Settings


async def test_concurrent_migrations_on_fresh_database_apply_each_version_once(
    postgres_container,
):
    """Replicas (or parallel test apps) starting together all migrate. Concurrent
    CREATE EXTENSION/TABLE IF NOT EXISTS race on the catalog unique index; the
    advisory lock must serialise them so every startup succeeds and each version
    is recorded exactly once."""
    pg = postgres_container
    base = f"postgresql://{pg.username}:{pg.password}@localhost:{int(pg.get_exposed_port(5432))}"
    dbname = f"schema_race_{uuid.uuid4().hex[:8]}"
    admin = await asyncpg.connect(f"{base}/{pg.dbname}")
    try:
        await admin.execute(f'CREATE DATABASE "{dbname}"')
        pools = [
            await asyncpg.create_pool(f"{base}/{dbname}", min_size=1, max_size=1)
            for _ in range(6)
        ]
        try:
            applied = await asyncio.gather(*(migrate_postgres(p) for p in pools))
            versions = [m.version for m in load_migrations("postgres")]
            # Exactly one replica applied everything; the rest found nothing to do.
            assert sorted(applied, key=len) == [[]] * 5 + [versions]
            async with pools[0].acquire() as conn:
                recorded = await conn.fetch("SELECT version FROM schema_migrations")
                assert sorted(r["version"] for r in recorded) == versions
                assert await conn.fetchval(
                    "SELECT to_regclass('configuration') IS NOT NULL"
                )
            assert await migrate_postgres(pools[0]) == []
        finally:
            await asyncio.gather(*(pool.close() for pool in pools))
    finally:
        await admin.execute(f'DROP DATABASE IF EXISTS "{dbname}" WITH (FORCE)')
        await admin.close()


async def test_clickhouse_migrations_apply_on_a_fresh_database_then_no_op(
    clickhouse_container, test_clickhouse_client
):
    """A fresh ClickHouse needs no compose init script: the app's client creates
    the schema on connect, and connecting again applies nothing."""
    dbname = f"fresh_{uuid.uuid4().hex[:8]}"
    await test_clickhouse_client.command(f"CREATE DATABASE {dbname}")
    settings = Settings(
        clickhouse_host="localhost",
        clickhouse_port=int(clickhouse_container.get_exposed_port(8123)),
        clickhouse_user=clickhouse_container.username or "default",
        clickhouse_password=SecretStr(clickhouse_container.password or ""),
        clickhouse_database=dbname,
    )
    try:
        async with ClickHouseClient(settings) as client:
            count = await client.query("SELECT count() FROM items")
            assert count.first_row == (0,)
            assert await migrate_clickhouse(client) == []
    finally:
        await test_clickhouse_client.command(f"DROP DATABASE IF EXISTS {dbname}")
