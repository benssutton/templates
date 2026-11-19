import asyncpg

from core.retry import connect_with_backoff
from persistence.migrations import migrate_postgres
from settings import Settings


class PostgresClient:
    """Async context manager yielding a live pool with migrations applied."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._pool: asyncpg.Pool | None = None

    async def __aenter__(self) -> asyncpg.Pool:
        pool = await connect_with_backoff(
            lambda: asyncpg.create_pool(
                self._settings.postgres_url.get_secret_value(),
                min_size=self._settings.postgres_pool_min_size,
                max_size=self._settings.postgres_pool_max_size,
                command_timeout=self._settings.postgres_command_timeout_seconds,
                timeout=self._settings.postgres_acquire_timeout_seconds,  # connect
            ),
            label="Postgres",
            max_attempts=self._settings.connect_max_attempts,
            base_delay=self._settings.connect_base_delay,
            max_delay=self._settings.connect_max_delay,
        )
        self._pool = pool
        try:
            await migrate_postgres(pool)
        except BaseException:
            await self.__aexit__()
            raise
        return pool

    async def __aexit__(self, *_: object) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
