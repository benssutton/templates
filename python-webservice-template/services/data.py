from clickhouse_connect.driver.asyncclient import AsyncClient

from core.boundary_timing import timed
from schemas.data import DataRowResponse, DataRowsResponse


class DataService:
    def __init__(self, client: AsyncClient):
        self._client = client

    async def get_data(self, limit: int) -> DataRowsResponse:
        async with timed("clickhouse.count"):
            count_result = await self._client.query("SELECT count() FROM items")
        total = count_result.first_row[0] if count_result.first_row else 0

        async with timed("clickhouse.select"):
            result = await self._client.query(
                "SELECT id, name, value FROM items ORDER BY id LIMIT %(limit)s",
                parameters={"limit": limit},
            )
        rows = [
            DataRowResponse(id=row[0], name=row[1], value=row[2])
            for row in result.result_rows
        ]
        return DataRowsResponse(rows=rows, total=total, limit=limit)

    async def ping(self) -> None:
        if not await self._client.ping():
            raise ConnectionError("ClickHouse ping returned False")
