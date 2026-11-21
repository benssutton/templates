import asyncpg

from core.correlation import timed
from schemas.config import ConfigEntry


class ConfigService:
    def __init__(self, pool: asyncpg.Pool, acquire_timeout: float = 5.0) -> None:
        self._pool = pool
        self._acquire_timeout = acquire_timeout

    async def get_all(self) -> list[ConfigEntry]:
        async with (
            timed("postgres.config.get_all"),
            self._pool.acquire(timeout=self._acquire_timeout) as conn,
        ):
            rows = await conn.fetch("SELECT key, value FROM configuration ORDER BY key")
        return [ConfigEntry(key=row["key"], value=row["value"]) for row in rows]

    async def set(self, key: str, value: str) -> ConfigEntry:
        async with (
            timed("postgres.config.set"),
            self._pool.acquire(timeout=self._acquire_timeout) as conn,
        ):
            row = await conn.fetchrow(
                """
                INSERT INTO configuration (key, value)
                VALUES ($1, $2)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                RETURNING key, value
                """,
                key,
                value,
            )
        if row is None:  # an upsert with RETURNING always yields the row
            raise RuntimeError("configuration upsert returned no row")
        return ConfigEntry(key=row["key"], value=row["value"])

    async def ping(self) -> None:
        async with self._pool.acquire(timeout=self._acquire_timeout) as conn:
            await conn.execute("SELECT 1")
