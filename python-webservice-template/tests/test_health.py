import asyncio

import pytest

from core.last_request import LastRequest
from ingestion.noop.client import NoopBatchConsumer
from persistence.stream_store.lsm_store import LSMStore
from services.health import HealthService
from services.stream_ingest import StreamIngestService
from settings import Settings


async def test_liveness_returns_alive(test_client):
    response = await test_client.get("/health/live")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "alive"
    assert body["uptime_seconds"] >= 0.0


async def test_status_reports_app_and_system(test_client):
    """status is overridden to 'testing' in the test fixtures."""
    response = await test_client.get("/health/status")
    assert response.status_code == 200
    body = response.json()
    assert body["app"]["status"] == "testing"
    assert body["uptime"]["process_seconds"] >= 0.0
    assert body["system"]["process"]["memory_rss_bytes"] > 0
    assert isinstance(body["dependencies"], list)
    assert body["ingest"]["transport"] == "noop"


async def test_root_returns_non_empty_json(test_client):
    response = await test_client.get("/")
    assert response.status_code == 200
    assert response.json()


def _health(settings: Settings, pings) -> HealthService:
    ingest = StreamIngestService(
        NoopBatchConsumer(settings), LSMStore(100, 4), settings
    )
    return HealthService(
        settings, pings=pings, ingest=ingest, last_request=LastRequest()
    )


@pytest.mark.parametrize(("ttl", "expected_pings"), [(60.0, 1), (0.0, 3)])
async def test_dependency_probes_are_cached_for_the_ttl(ttl, expected_pings):
    calls: list[str] = []

    async def ping() -> None:
        calls.append("ping")

    settings = Settings(ingest_transport="noop", health_cache_ttl_seconds=ttl)
    health = _health(settings, {"store": ping})
    for _ in range(3):
        await health.dependencies()
    assert len(calls) == expected_pings


async def test_concurrent_callers_share_one_probe_round():
    calls: list[str] = []

    async def slow_ping() -> None:
        calls.append("ping")
        await asyncio.sleep(0.05)

    settings = Settings(ingest_transport="noop", health_cache_ttl_seconds=60.0)
    health = _health(settings, {"store": slow_ping})
    await asyncio.gather(*(health.dependencies() for _ in range(5)))
    assert calls == ["ping"]


async def test_probe_timeout_is_reported_as_timeout():
    async def hangs() -> None:
        await asyncio.sleep(10)

    settings = Settings(ingest_transport="noop", health_check_timeout_seconds=0.05)
    [result] = await _health(settings, {"store": hangs}).dependencies()
    assert (result.status, result.error) == ("down", "timeout")
