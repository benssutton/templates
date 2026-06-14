import time

import pytest

from settings import Settings
from tests.app_client import lifespan_test_client

pytestmark = pytest.mark.resilience


async def test_clickhouse_unreachable_aborts_startup_after_bounded_retries(
    postgres_container, redis_container,
):
    # Real Postgres + Redis (both connect on the first try); ClickHouse points at
    # a closed port. With a tiny, Settings-driven backoff budget the real lifespan
    # must retry then abort — not hang, not skip the failure.
    pg_port = int(postgres_container.get_exposed_port(5432))
    redis_port = int(redis_container.get_exposed_port(6379))
    settings = Settings(
        postgres_url=f"postgresql://{postgres_container.username}:{postgres_container.password}@localhost:{pg_port}/{postgres_container.dbname}",
        redis_url=f"redis://localhost:{redis_port}/0",
        clickhouse_host="127.0.0.1",
        clickhouse_port=1,                 # nothing is listening here
        connect_max_attempts=3,
        connect_base_delay=0.001,
        connect_max_delay=0.005,
        ingest_max_disconnect_seconds=None,
    )
    start = time.monotonic()
    # Broad Exception: connect_with_backoff re-raises the last attempt's
    # exception, which varies by library version (aiohttp, clickhouse_connect).
    # The assertion below validates the test does not hang, which is the core invariant.
    with pytest.raises(Exception):
        async with lifespan_test_client(settings):
            pass
    assert time.monotonic() - start < 15.0  # retries ≤15ms total; 15s allows for container/ASGI startup overhead
