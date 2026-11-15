import asyncio
import logging
import math
import threading

import pytest
from testcontainers.clickhouse import ClickHouseContainer
from testcontainers.redis import RedisContainer

from settings import Settings
from tests.app_client import lifespan_test_client
from tests.publishers.batches import make_batch
from tests.support.settings import app_settings
from tests.support.solace import broker_paused, wait_for_queue
from tests.support.wait import check, poll_cache, poll_json, poll_ready

pytestmark = pytest.mark.observability

REDIS_IMAGE = "redis/redis-stack-server:latest"
CH_IMAGE = "clickhouse/clickhouse-server:latest"
BATCHES = [
    make_batch([(1, "a", "v1", "upsert"), (2, "b", "v1", "upsert")]),
    make_batch([(3, "c", "v1", "upsert")]),
    make_batch([(4, "d", "v1", "upsert")]),
]


async def test_liveness_always_200(
    postgres_container, clickhouse_container, test_clickhouse_client, redis_container
):
    async with lifespan_test_client(
        app_settings(postgres_container, clickhouse_container, redis_container)
    ) as client:
        resp = await client.get("/health/live")
        assert resp.status_code == 200
        assert resp.json()["status"] == "alive"


async def test_readiness_all_up_returns_200(solace_settings):
    async with lifespan_test_client(solace_settings()) as client:
        resp = await poll_ready(client, 200)
        body = resp.json()
        assert body["status"] == "ready"
        for dep in ("postgres", "clickhouse", "redis", "ingest"):
            assert check(body, dep)["status"] == "up"
        assert check(body, "ingest")["connection_state"] == "connected"


async def test_status_structure_and_ingest_freshness(
    solace_settings, solace_names, semp, solace_publisher
):
    async with lifespan_test_client(solace_settings()) as client:
        await wait_for_queue(semp, solace_names.queue)
        solace_publisher.publish_batch(BATCHES[0])
        await poll_cache(client, expected_total=2)
        # The store becomes visible a moment before the ingest counters update.
        _, body = await poll_json(
            client,
            "/health/status",
            lambda r, b: r.is_success
            and (b.get("ingest") or {}).get("rows_ingested_total") == 2,
            timeout=10.0,
            what="rows_ingested_total == 2",
        )
        assert body["app"]["status"] == "testing"
        assert body["uptime"]["process_seconds"] >= 0.0
        assert body["system"]["process"]["memory_rss_bytes"] > 0
        assert body["ingest"]["transport"] == "solace"
        assert body["ingest"]["rows_ingested_total"] == 2
        assert body["ingest"]["last_batch_at"] is not None


async def test_metrics_endpoint_exposes_series(
    postgres_container, clickhouse_container, test_clickhouse_client, redis_container
):
    async with lifespan_test_client(
        app_settings(postgres_container, clickhouse_container, redis_container)
    ) as client:
        await client.get("/health/live")  # generate one request metric
        resp = await client.get("/metrics")
        assert resp.status_code == 200
        assert "text/plain" in resp.headers["content-type"]
        for series in (
            "http_request_duration_seconds",
            "dependency_up",
            "ingest_connection_state",
            "ingest_seconds_since_last_batch",
            "process_memory_rss_bytes",
        ):
            assert series in resp.text


async def test_broker_partition_fails_readiness_then_recovers(
    solace_settings, solace_container
):
    # watchdog off: this test is about readiness, not shutdown
    settings = solace_settings(ingest_max_disconnect_seconds=None)
    async with lifespan_test_client(settings) as client:
        await poll_ready(client, 200)
        with broker_paused(solace_container):
            # Wait for the partition to be the cause of the 503, not merely
            # the first 503 (SDK keepalive needs ~30-35 s to notice).
            resp = await poll_ready(
                client,
                503,
                until=lambda b: check(b, "ingest")["connection_state"]
                == "reconnecting",
                timeout=90.0,
            )
            ingest = check(resp.json(), "ingest")
            assert ingest["connection_state"] == "reconnecting"
            assert ingest["status"] == "down"
        await poll_ready(client, 200, timeout=90.0)


async def test_idle_ingest_is_stale_but_ready():
    """Connected-but-idle ingest reports stale=True without failing readiness
    when ingest_stale_fails_readiness=False. Transport-agnostic: an idle
    in-process consumer, no broker needed."""
    from core.container import Container
    from ingestion.base import ConnectionState
    from persistence.stream_store.lsm_store import LSMStore
    from services.health import HealthService
    from services.stream_ingest import StreamIngestService

    class _IdleConsumer:
        def deliveries(self):
            threading.Event().wait()
            yield from ()

        def close(self):
            pass

        def connection_state(self):
            return ConnectionState.CONNECTED

    settings = Settings(
        ingest_transport="noop",
        ingest_staleness_threshold_seconds=2.0,
        ingest_stale_fails_readiness=False,
        ingest_max_disconnect_seconds=None,
    )
    svc = StreamIngestService(
        _IdleConsumer(), LSMStore(flush_rows=100, compaction_runs=4), settings
    )

    health = await svc.health_check()
    assert health.connection_state == "connected"
    assert health.stale is False
    await asyncio.sleep(2.5)
    health2 = await svc.health_check()
    assert health2.connection_state == "connected"
    assert health2.stale is True

    container = Container(settings)
    container.register_singleton(StreamIngestService, svc)
    readiness = await HealthService(settings, container).readiness()
    ingest_check = next(c for c in readiness.checks if c.name == "ingest")
    assert ingest_check.status == "up"
    assert readiness.status == "not_ready"  # deps not available, but ingest is up


async def test_dependency_down_fails_readiness(
    postgres_container, clickhouse_container, test_clickhouse_client
):
    dedicated_redis = RedisContainer(REDIS_IMAGE)
    dedicated_redis.start()
    try:
        async with lifespan_test_client(
            app_settings(postgres_container, clickhouse_container, dedicated_redis)
        ) as client:
            await poll_ready(client, 200)
            dedicated_redis.stop()
            resp = await poll_ready(client, 503, timeout=15.0)
            assert resp.status_code == 503
            assert {c["name"]: c for c in resp.json()["checks"]}["redis"][
                "status"
            ] == "down"
    finally:
        try:
            dedicated_redis.stop()
        except Exception:
            pass


async def test_health_probe_error_is_generic_not_leaky(
    postgres_container, clickhouse_container, test_clickhouse_client
):
    dedicated_redis = RedisContainer(REDIS_IMAGE)
    dedicated_redis.start()
    try:
        async with lifespan_test_client(
            app_settings(postgres_container, clickhouse_container, dedicated_redis)
        ) as client:
            await poll_ready(client, 200)
            dedicated_redis.stop()
            resp = await poll_ready(client, 503, timeout=15.0)
            redis_check = {c["name"]: c for c in resp.json()["checks"]}["redis"]
            assert redis_check["status"] == "down"
            # CacheService returns "unavailable", or the probe's wait_for trips
            # first with "timeout"; never the raw exception string.
            error = redis_check["error"]
            assert error in {"unavailable", "timeout"}
            assert "localhost" not in error and "Error" not in error
    finally:
        try:
            dedicated_redis.stop()
        except Exception:
            pass


async def test_clickhouse_health_check_exception_returns_generic_error(
    postgres_container, redis_container
):
    dedicated_ch = ClickHouseContainer(CH_IMAGE, port=8123)
    dedicated_ch.start()
    try:
        async with lifespan_test_client(
            app_settings(postgres_container, dedicated_ch, redis_container)
        ) as client:
            await poll_ready(client, 200)
            dedicated_ch.stop()
            resp = await poll_ready(client, 503, timeout=30.0)
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


async def test_postgres_health_check_exception_logs_and_returns_generic_error(
    postgres_container, caplog
):
    import asyncpg
    from services.config import ConfigService

    pg = postgres_container
    pool = await asyncpg.create_pool(
        f"postgresql://{pg.username}:{pg.password}@localhost:{int(pg.get_exposed_port(5432))}/{pg.dbname}",
        min_size=0,
        max_size=1,
    )
    await pool.close()  # closed pool -> acquire() raises InterfaceError
    svc = ConfigService(pool)
    with caplog.at_level(logging.ERROR, logger="services.config"):
        result = await svc.health_check()
    assert result.status == "down"
    assert result.error == "unavailable"
    assert "postgres health check failed" in caplog.text


async def test_streaming_assigns_distinct_per_message_ids(
    solace_settings, solace_names, semp, solace_publisher, cid_caplog
):
    with cid_caplog.at_level(logging.DEBUG):
        async with lifespan_test_client(solace_settings()) as client:
            await wait_for_queue(semp, solace_names.queue)
            for batch in BATCHES:
                solace_publisher.publish_batch(batch)
            await poll_cache(client, expected_total=4)
    ingest_logs = [r for r in cid_caplog.records if "ingested batch" in r.getMessage()]
    cids = {r.correlation_id for r in ingest_logs}
    assert len(ingest_logs) == 3
    assert "-" not in cids  # every message got a real ID
    assert len(cids) == 3  # and a DISTINCT one per message


async def test_ingest_never_started_metric_is_nan():
    """ingest_seconds_since_last_batch must be NaN (not 0.0) before the first
    batch so monitoring can distinguish 'never ingested' from 'just ingested'."""
    from schemas.health import (
        AppInfo,
        DetailedStatusResponse,
        HostStats,
        IngestHealth,
        ProcessStats,
        RequestInfo,
        SystemSnapshot,
        UptimeInfo,
    )
    from services.metrics import MetricsService

    class _FakeHealthService:
        async def detailed_status(self):
            return DetailedStatusResponse(
                app=AppInfo(title="t", version="1", status="testing"),
                uptime=UptimeInfo(process_seconds=1.0, system_boot_seconds=0.0),
                dependencies=[],
                ingest=IngestHealth(
                    transport="solace",
                    connection_state="connected",
                    thread_alive=True,
                    seconds_since_last_batch=None,
                    rows_ingested_total=0,
                ),
                requests=RequestInfo(),
                system=SystemSnapshot(
                    process=ProcessStats(
                        cpu_percent=0.0, memory_rss_bytes=0, num_threads=0, open_files=0
                    ),
                    host=HostStats(
                        cpu_percent=0.0,
                        memory_total_bytes=0,
                        memory_available_bytes=0,
                        memory_percent=0.0,
                    ),
                ),
            )

    metrics = MetricsService(Settings())
    await metrics.refresh(_FakeHealthService())
    text = metrics.render()[0].decode()
    secs_line = next(
        (
            ln
            for ln in text.splitlines()
            if ln.startswith("ingest_seconds_since_last_batch ")
        ),
        None,
    )
    assert secs_line is not None, "ingest_seconds_since_last_batch metric not found"
    assert math.isnan(float(secs_line.split(" ")[1]))


async def test_get_data_logs_clickhouse_timing(
    postgres_container,
    clickhouse_container,
    test_clickhouse_client,
    redis_container,
    cid_caplog,
):
    with cid_caplog.at_level(logging.DEBUG, logger="core.correlation"):
        async with lifespan_test_client(
            app_settings(postgres_container, clickhouse_container, redis_container)
        ) as client:
            resp = await client.get("/data?limit=5")
            assert resp.status_code == 200
    assert any("clickhouse.select" in r.getMessage() for r in cid_caplog.records)
