import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient

from core.retry import connect_with_backoff
from settings import Settings


class ClickHouseClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: AsyncClient | None = None

    async def __aenter__(self) -> AsyncClient:
        async def _connect() -> AsyncClient:
            client = await clickhouse_connect.get_async_client(
                host=self._settings.clickhouse_host,
                port=self._settings.clickhouse_port,
                username=self._settings.clickhouse_user,
                password=self._settings.clickhouse_password.get_secret_value(),
                database=self._settings.clickhouse_database,
            )
            if not await client.ping():           # smoke-test: raises on failure -> retried
                await client.close()
                raise ConnectionError("ClickHouse startup ping returned False")
            return client

        self._client = await connect_with_backoff(
            _connect,
            label="ClickHouse",
            max_attempts=self._settings.connect_max_attempts,
            base_delay=self._settings.connect_base_delay,
            max_delay=self._settings.connect_max_delay,
        )
        return self._client

    async def __aexit__(self, *_: object) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None
