"""A store outage on a request path answers 503 {"detail": "Service Unavailable"}
within a bounded time, instead of hanging or leaking a 500."""

import time

from testcontainers.redis import RedisContainer

from tests.app_client import lifespan_test_client
from tests.support.images import REDIS_IMAGE
from tests.support.settings import app_settings

SOCKET_TIMEOUT = 1.0


async def test_paused_then_stopped_redis_returns_bounded_503(
    postgres_container, clickhouse_container, test_clickhouse_client
):
    redis = RedisContainer(REDIS_IMAGE)
    redis.start()
    try:
        settings = app_settings(
            postgres_container,
            clickhouse_container,
            redis,
            redis_socket_timeout_seconds=SOCKET_TIMEOUT,
            redis_connect_timeout_seconds=SOCKET_TIMEOUT,
        )
        async with lifespan_test_client(settings) as client:
            assert (
                await client.post("/cache", json={"key": "k", "value": 1})
            ).status_code == 201

            wrapped = redis.get_wrapped_container()
            wrapped.pause()  # frozen: the socket stays open, replies never come
            try:
                started = time.monotonic()
                response = await client.get("/cache/k")
                elapsed = time.monotonic() - started
            finally:
                wrapped.unpause()
            assert response.status_code == 503
            assert response.json() == {"detail": "Service Unavailable"}
            assert elapsed < SOCKET_TIMEOUT * 3

            redis.stop()  # gone: connections are refused
            started = time.monotonic()
            response = await client.get("/cache/k")
            assert response.status_code == 503
            assert response.json() == {"detail": "Service Unavailable"}
            assert time.monotonic() - started < SOCKET_TIMEOUT * 3
    finally:
        try:
            redis.stop()
        except Exception:
            pass
