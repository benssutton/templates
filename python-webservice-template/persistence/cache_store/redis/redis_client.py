import redis.asyncio as aioredis

from core.retry import connect_with_backoff
from settings import Settings


class RedisClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: aioredis.Redis | None = None

    async def __aenter__(self) -> aioredis.Redis:
        async def _connect() -> aioredis.Redis:
            client = aioredis.Redis.from_url(self._settings.redis_url.get_secret_value())
            await client.ping()          # smoke-test: raises if Redis is unreachable
            await self._assert_json_module(client)
            return client

        self._client = await connect_with_backoff(
            _connect,
            label="Redis",
            max_attempts=self._settings.connect_max_attempts,
            base_delay=self._settings.connect_base_delay,
            max_delay=self._settings.connect_max_delay,
        )
        return self._client

    @staticmethod
    async def _assert_json_module(client: aioredis.Redis) -> None:
        """Fail fast if the server lacks the RedisJSON module the cache relies on.

        CacheService uses JSON.SET/JSON.GET; stock Redis returns 'unknown command'
        only at first write. Probing here surfaces the misconfiguration at startup.
        """
        probe_key = "__startup_json_probe__"
        try:
            await client.json().set(probe_key, "$", {"ok": True})
            await client.delete(probe_key)
        except aioredis.ResponseError as exc:
            raise RuntimeError(
                "Redis is reachable but the RedisJSON module is missing. "
                "This template's cache requires redis-stack (e.g. the "
                "redis/redis-stack-server image). Original error: " + str(exc)
            ) from exc

    async def __aexit__(self, *_: object) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
