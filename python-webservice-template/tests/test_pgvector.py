import asyncpg


async def test_pgvector_extension_is_installed(test_client, postgres_container):
    """The lifespan runs postgres-init.sql on startup; after that the `vector`
    extension must exist. Guards against a revert to a non-pgvector image
    (CREATE EXTENSION would fail there and abort startup)."""
    pg = postgres_container
    conn = await asyncpg.connect(
        f"postgresql://{pg.username}:{pg.password}@localhost:"
        f"{int(pg.get_exposed_port(5432))}/{pg.dbname}"
    )
    try:
        version = await conn.fetchval(
            "SELECT extversion FROM pg_extension WHERE extname = 'vector'"
        )
    finally:
        await conn.close()
    assert version is not None
