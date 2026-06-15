import threading

import pyarrow.flight as pa_flight
import pytest
from httpx import AsyncClient
from testcontainers.redis import RedisContainer

from settings import Settings
from tests.app_client import lifespan_test_client
from tests.publishers.flight_server import ExampleFlightServer, make_batch

pytestmark = pytest.mark.observability

REDIS_IMAGE = "redis/redis-stack-server:latest"

_STREAM_SCRIPT = [make_batch([(1, "a", "v1", "upsert"), (2, "b", "v1", "upsert")])]


def _settings(pg, ch, redis_url, flight_port, **overrides) -> Settings:
    return Settings(
        status="testing",
        postgres_url=f"postgresql://{pg.username}:{pg.password}@localhost:{int(pg.get_exposed_port(5432))}/{pg.dbname}",
        clickhouse_host="localhost",
        clickhouse_port=int(ch.get_exposed_port(8123)),
        clickhouse_user=ch.username or "default",
        clickhouse_password=ch.password or "",
        clickhouse_database="default",
        redis_url=redis_url,
        ingest_transport="flight",
        flight_host="localhost",
        flight_port=flight_port,
        flight_ticket="items",
        lsm_flush_rows=2,
        lsm_compaction_runs=2,
        **overrides,
    )


@pytest.fixture(scope="module")
def streaming_flight_server():
    location = pa_flight.Location.for_grpc_tcp("localhost", 0)
    server = ExampleFlightServer(location, _STREAM_SCRIPT, interval=0.05, loop=True)
    threading.Thread(target=server.serve, daemon=True).start()
    yield server
    server.shutdown()


async def _poll_ready(client: AsyncClient, expected_code: int, timeout: float = 10.0):
    import asyncio
    import time
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = await client.get("/health/ready")
        if last.status_code == expected_code:
            return last
        await asyncio.sleep(0.1)
    return last


async def test_liveness_always_200(
    postgres_container, clickhouse_container, test_clickhouse_client,
    redis_container, streaming_flight_server,
):
    settings = _settings(
        postgres_container, clickhouse_container,
        f"redis://localhost:{int(redis_container.get_exposed_port(6379))}/0",
        streaming_flight_server.port,
    )
    async with lifespan_test_client(settings) as client:
        resp = await client.get("/health/live")
        assert resp.status_code == 200
        assert resp.json()["status"] == "alive"


async def test_readiness_all_up_returns_200(
    postgres_container, clickhouse_container, test_clickhouse_client,
    redis_container, streaming_flight_server,
):
    settings = _settings(
        postgres_container, clickhouse_container,
        f"redis://localhost:{int(redis_container.get_exposed_port(6379))}/0",
        streaming_flight_server.port,
    )
    async with lifespan_test_client(settings) as client:
        resp = await _poll_ready(client, 200)
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ready"
        by_name = {c["name"]: c for c in body["checks"]}
        assert by_name["postgres"]["status"] == "up"
        assert by_name["clickhouse"]["status"] == "up"
        assert by_name["redis"]["status"] == "up"
        assert by_name["ingest"]["status"] == "up"
        assert by_name["ingest"]["connection_state"] == "connected"


async def test_status_structure_and_ingest_freshness(
    postgres_container, clickhouse_container, test_clickhouse_client,
    redis_container, streaming_flight_server,
):
    settings = _settings(
        postgres_container, clickhouse_container,
        f"redis://localhost:{int(redis_container.get_exposed_port(6379))}/0",
        streaming_flight_server.port,
    )
    async with lifespan_test_client(settings) as client:
        await _poll_ready(client, 200)
        body = (await client.get("/health/status")).json()
        assert body["app"]["status"] == "testing"
        assert body["uptime"]["process_seconds"] >= 0.0
        assert body["system"]["process"]["memory_rss_bytes"] > 0
        assert body["ingest"]["rows_ingested_total"] > 0
        assert body["ingest"]["last_batch_at"] is not None


async def test_metrics_endpoint_exposes_series(
    postgres_container, clickhouse_container, test_clickhouse_client,
    redis_container, streaming_flight_server,
):
    settings = _settings(
        postgres_container, clickhouse_container,
        f"redis://localhost:{int(redis_container.get_exposed_port(6379))}/0",
        streaming_flight_server.port,
    )
    async with lifespan_test_client(settings) as client:
        await client.get("/health/live")          # generate one request metric
        resp = await client.get("/metrics")
        assert resp.status_code == 200
        assert "text/plain" in resp.headers["content-type"]
        text = resp.text
        assert "http_request_duration_seconds" in text
        assert "dependency_up" in text
        assert "ingest_connection_state" in text
        assert "ingest_seconds_since_last_batch" in text
        assert "process_memory_rss_bytes" in text


async def test_ingest_disconnect_fails_readiness(
    postgres_container, clickhouse_container, test_clickhouse_client, redis_container,
):
    # Use a finite (non-looping) server: after exhausting the script, batches()
    # raises ConnectionError and the consumer enters RECONNECTING state.
    # Because the server is still running, each reconnection cycle resets the
    # failure counter (one batch succeeds then the stream ends again), so the
    # 5-failure SIGTERM is never triggered.
    location = pa_flight.Location.for_grpc_tcp("localhost", 0)
    server = ExampleFlightServer(location, _STREAM_SCRIPT, interval=0.0, loop=False)
    threading.Thread(target=server.serve, daemon=True).start()
    settings = _settings(
        postgres_container, clickhouse_container,
        f"redis://localhost:{int(redis_container.get_exposed_port(6379))}/0",
        server.port,
        ingest_max_disconnect_seconds=None,   # watchdog disabled; avoid SIGTERM in tests
    )
    async with lifespan_test_client(settings) as client:
        # Stream ends immediately; consumer enters RECONNECTING → readiness 503.
        resp = await _poll_ready(client, 503)
        assert resp.status_code == 503
        ingest = {c["name"]: c for c in resp.json()["checks"]}["ingest"]
        assert ingest["connection_state"] != "connected"
    server.shutdown()


async def test_idle_ingest_is_stale_but_ready(
    postgres_container, clickhouse_container, test_clickhouse_client,
    redis_container, streaming_flight_server,
):
    """Connected ingest that is idle (no recent batches) reports stale=True but
    does not fail readiness when ingest_stale_fails_readiness=False.

    Uses a streaming server (loop=True) to keep the consumer CONNECTED, sets a
    very short staleness threshold, then disables ingest metrics to prevent the
    server from racing ahead before the stale check.  We verify behaviour via
    the HealthService directly rather than through a real-server teardown that
    can block the gRPC stream indefinitely on Windows.
    """
    import asyncio
    from ingestion.base import ConnectionState
    from persistence.stream_store.lsm_store import LSMStore
    from services.health import HealthService
    from services.stream_ingest import StreamIngestService
    from core.container import Container

    # A fake consumer that is CONNECTED but never produces batches.
    class _IdleConsumer:
        def batches(self):
            threading.Event().wait()    # blocks forever until close()
            return
            yield
        def close(self): pass
        def connection_state(self): return ConnectionState.CONNECTED

    settings = Settings(
        ingest_transport="flight",
        ingest_staleness_threshold_seconds=0.5,
        ingest_stale_fails_readiness=False,
        ingest_max_disconnect_seconds=None,
    )
    store = LSMStore(flush_rows=100, compaction_runs=4)
    svc = StreamIngestService(_IdleConsumer(), store, settings)

    # health_check does not require the ingest thread to be running.
    health = await svc.health_check()
    assert health.connection_state == "connected"
    assert health.stale is False    # not yet past threshold

    await asyncio.sleep(0.6)
    health2 = await svc.health_check()
    assert health2.connection_state == "connected"
    assert health2.stale is True    # past 0.5 s threshold, no batch ever received

    # Confirm that stale-but-connected does NOT fail the HealthService readiness
    # check (ingest_stale_fails_readiness=False).
    container = Container(settings)
    container.register_singleton(StreamIngestService, svc)
    health_svc = HealthService(settings, container)
    readiness = await health_svc.readiness()
    ingest_check = next(c for c in readiness.checks if c.name == "ingest")
    assert ingest_check.status == "up"
    assert readiness.status == "not_ready"   # deps not available, but ingest is up


async def test_dependency_down_fails_readiness(
    postgres_container, clickhouse_container, test_clickhouse_client, streaming_flight_server,
):
    dedicated_redis = RedisContainer(REDIS_IMAGE)
    dedicated_redis.start()
    try:
        redis_url = f"redis://localhost:{int(dedicated_redis.get_exposed_port(6379))}/0"
        settings = _settings(
            postgres_container, clickhouse_container, redis_url, streaming_flight_server.port,
        )
        async with lifespan_test_client(settings) as client:
            assert (await _poll_ready(client, 200)).status_code == 200
            dedicated_redis.stop()             # kill only this test's redis
            resp = await _poll_ready(client, 503, timeout=15.0)
            assert resp.status_code == 503
            redis_check = {c["name"]: c for c in resp.json()["checks"]}["redis"]
            assert redis_check["status"] == "down"
    finally:
        try:
            dedicated_redis.stop()
        except Exception:
            pass


async def test_health_probe_error_is_generic_not_leaky(
    postgres_container, clickhouse_container, test_clickhouse_client, streaming_flight_server,
):
    dedicated_redis = RedisContainer(REDIS_IMAGE)
    dedicated_redis.start()
    try:
        redis_url = f"redis://localhost:{int(dedicated_redis.get_exposed_port(6379))}/0"
        settings = _settings(
            postgres_container, clickhouse_container, redis_url, streaming_flight_server.port,
        )
        async with lifespan_test_client(settings) as client:
            assert (await _poll_ready(client, 200)).status_code == 200
            dedicated_redis.stop()
            resp = await _poll_ready(client, 503, timeout=15.0)
            assert resp.status_code == 503
            redis_check = {c["name"]: c for c in resp.json()["checks"]}["redis"]
            assert redis_check["status"] == "down"
            # Two non-leaky paths can fire: CacheService catches the error and
            # returns "unavailable"; or, when the dead connection hangs, the
            # probe's wait_for trips first and returns "timeout". Both are
            # intentional generic tokens — the point of the finding is that the
            # raw exception string (host/port/errno) is never surfaced.
            error = redis_check["error"]
            assert error in {"unavailable", "timeout"}
            assert "localhost" not in error and "Error" not in error
    finally:
        try:
            dedicated_redis.stop()
        except Exception:
            pass


async def test_clickhouse_health_check_exception_returns_generic_error(
    postgres_container, redis_container, streaming_flight_server,
):
    # clickhouse_connect.ping() raises OperationalError on a real network refusal
    # (rather than returning False), so the except branch in DataService.health_check
    # requires a genuine connection failure.  Pattern mirrors the Redis test above:
    # dedicated container, start app, kill container, confirm generic error in probe.
    from testcontainers.clickhouse import ClickHouseContainer
    dedicated_ch = ClickHouseContainer("clickhouse/clickhouse-server:latest", port=8123)
    dedicated_ch.start()
    try:
        settings = _settings(
            postgres_container,
            dedicated_ch,
            f"redis://localhost:{int(redis_container.get_exposed_port(6379))}/0",
            streaming_flight_server.port,
        )
        async with lifespan_test_client(settings) as client:
            assert (await _poll_ready(client, 200)).status_code == 200
            dedicated_ch.stop()
            resp = await _poll_ready(client, 503, timeout=30.0)
        assert resp.status_code == 503
        ch_check = {c["name"]: c for c in resp.json()["checks"]}["clickhouse"]
        assert ch_check["status"] == "down"
        error = ch_check.get("error", "")
        assert error in {"unavailable", "timeout"}
        assert "localhost" not in error and "Error" not in error
    finally:
        try:
            dedicated_ch.stop()
        except Exception:
            pass


async def test_postgres_health_check_exception_logs_and_returns_generic_error(postgres_container, caplog):
    import asyncpg
    import logging
    from services.config import ConfigService

    pg = postgres_container
    pool = await asyncpg.create_pool(
        f"postgresql://{pg.username}:{pg.password}@localhost:{int(pg.get_exposed_port(5432))}/{pg.dbname}",
        min_size=0,
        max_size=1,
    )
    await pool.close()   # closed pool → acquire() raises InterfaceError
    svc = ConfigService(pool)
    with caplog.at_level(logging.ERROR, logger="services.config"):
        result = await svc.health_check()

    assert result.status == "down"
    assert result.error == "unavailable"
    assert "postgres health check failed" in caplog.text


async def test_streaming_assigns_distinct_per_batch_ids(
    postgres_container, clickhouse_container, test_clickhouse_client,
    redis_container, streaming_flight_server, cid_caplog,
):
    import asyncio
    import logging
    settings = _settings(
        postgres_container, clickhouse_container,
        f"redis://localhost:{int(redis_container.get_exposed_port(6379))}/0",
        streaming_flight_server.port, ingest_max_disconnect_seconds=None,
    )
    with cid_caplog.at_level(logging.DEBUG):
        async with lifespan_test_client(settings) as client:
            await _poll_ready(client, 200)
            await asyncio.sleep(0.3)     # let several batches stream in
    ingest_logs = [r for r in cid_caplog.records if "ingested batch" in r.getMessage()]
    cids = {r.correlation_id for r in ingest_logs}
    assert len(ingest_logs) >= 2
    assert "-" not in cids               # every batch got a real ID
    assert len(cids) >= 2                # and a DISTINCT one per batch


async def test_ingest_never_started_metric_is_nan():
    """ingest_seconds_since_last_batch must be NaN (not 0.0) before the first
    batch arrives so monitoring systems can distinguish 'never ingested' from
    'just ingested'."""
    import math
    from schemas.health import (
        AppInfo, DetailedStatusResponse, HostStats, IngestHealth,
        ProcessStats, ProbeResult, RequestInfo, SystemSnapshot, UptimeInfo,
    )
    from services.metrics import MetricsService

    class _FakeHealthService:
        async def detailed_status(self):
            return DetailedStatusResponse(
                app=AppInfo(title="t", version="1", status="testing"),
                uptime=UptimeInfo(process_seconds=1.0, system_boot_seconds=0.0),
                dependencies=[],
                ingest=IngestHealth(
                    transport="flight",
                    connection_state="connected",
                    thread_alive=True,
                    seconds_since_last_batch=None,
                    rows_ingested_total=0,
                ),
                requests=RequestInfo(),
                system=SystemSnapshot(
                    process=ProcessStats(cpu_percent=0.0, memory_rss_bytes=0, num_threads=0, open_files=0),
                    host=HostStats(cpu_percent=0.0, memory_total_bytes=0, memory_available_bytes=0, memory_percent=0.0),
                ),
            )

    from settings import Settings
    metrics = MetricsService(Settings())
    await metrics.refresh(_FakeHealthService())

    text = metrics.render()[0].decode()
    secs_line = next(
        (l for l in text.splitlines() if l.startswith("ingest_seconds_since_last_batch ")),
        None,
    )
    assert secs_line is not None, "ingest_seconds_since_last_batch metric not found"
    raw_value = secs_line.split(" ")[1]
    assert math.isnan(float(raw_value)), (
        f"expected NaN for never-ingested gauge, got {raw_value!r}"
    )


async def test_get_data_logs_clickhouse_timing(
    postgres_container, clickhouse_container, test_clickhouse_client,
    redis_container, streaming_flight_server, cid_caplog,
):
    import logging
    settings = _settings(
        postgres_container, clickhouse_container,
        f"redis://localhost:{int(redis_container.get_exposed_port(6379))}/0",
        streaming_flight_server.port, ingest_max_disconnect_seconds=None,
    )
    with cid_caplog.at_level(logging.DEBUG, logger="core.correlation"):
        async with lifespan_test_client(settings) as client:
            resp = await client.get("/data?limit=5")
            assert resp.status_code == 200
    assert any("clickhouse.select" in r.getMessage() for r in cid_caplog.records)
