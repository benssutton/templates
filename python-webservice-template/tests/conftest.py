from pathlib import Path

import pyarrow.ipc as pa_ipc
import pytest
from pydantic import SecretStr
from testcontainers.clickhouse import ClickHouseContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

from persistence.analytics_store.clickhouse.clickhouse_client import ClickHouseClient
from settings import Settings
from tests.app_client import lifespan_test_client
from tests.publishers.solace_publisher import SolacePublisher
from tests.support.images import CH_IMAGE, PG_IMAGE, REDIS_IMAGE
from tests.support.semp import Semp
from tests.support.settings import (
    SolaceNames,
    app_settings,
    new_solace_names,
    solace_overrides,
)
from tests.support.solace import (
    semp_url,
    solace_broker,
    wait_for_solace_messaging,
    wait_for_solace_ready,
)


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(items):
    # One broker per worker would cost ~60 s each, and broker_paused() would
    # disturb other tests, so every Solace test runs on one worker.
    for item in items:
        if "solace_container" in getattr(item, "fixturenames", ()):
            item.add_marker(pytest.mark.xdist_group("solace"))


# ── ClickHouse fixtures ────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def clickhouse_container():
    with ClickHouseContainer(CH_IMAGE, port=8123) as ch:
        yield ch


@pytest.fixture(scope="session")
async def test_clickhouse_client(clickhouse_container):
    http_port = int(clickhouse_container.get_exposed_port(8123))
    ch_settings = Settings(
        clickhouse_host="localhost",
        clickhouse_port=http_port,
        clickhouse_user=clickhouse_container.username or "default",
        clickhouse_password=SecretStr(clickhouse_container.password or ""),
        clickhouse_database="default",
    )
    # ClickHouseClient applies migrations/clickhouse on entry, as the app does.
    async with ClickHouseClient(ch_settings) as client:
        with pa_ipc.open_file(
            Path(__file__).parent / "test_data" / "clickhouse_seed_data.ipc"
        ) as reader:
            arrow_table = reader.read_all()
        await client.insert_arrow("default.items", arrow_table)
        yield client


# ── Postgres fixtures ──────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer(PG_IMAGE) as pg:
        yield pg


# ── Redis fixtures ─────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def redis_container():
    with RedisContainer(REDIS_IMAGE) as r:
        yield r


# ── Async Test Client ──────────────────────────────────────────────────────
#
# Each client fixture builds its OWN isolated app via main.create_app — its
# own DI container, MCPServer instance, and lifespan — so multiple client
# fixtures (noop / HTTP-ingest / Solace) can coexist in one pytest process
# without sharing state.


@pytest.fixture(scope="session")
async def test_client(
    postgres_container, clickhouse_container, test_clickhouse_client, redis_container
):
    settings = app_settings(
        postgres_container, clickhouse_container, redis_container
    )  # noop transport
    async with lifespan_test_client(settings) as client:
        yield client


# ── Solace fixtures ────────────────────────────────────────────────────────
#
# ONE broker per test session (its POST + start takes ~60 s). Tests isolate
# themselves with a unique topic + queue from `solace_names`.


@pytest.fixture(scope="session")
def solace_container():
    with solace_broker() as container:
        wait_for_solace_ready(container)
        # guaranteed-active only proves the router is up; the message VPN can
        # still refuse SMF clients for a while. Wait for a real connect.
        wait_for_solace_messaging(container)
        yield container


@pytest.fixture(scope="session")
def semp(solace_container):
    client = Semp(semp_url(solace_container))
    yield client
    client.close()


@pytest.fixture
def solace_names() -> SolaceNames:
    return new_solace_names()


@pytest.fixture
def solace_settings(
    postgres_container,
    clickhouse_container,
    test_clickhouse_client,
    redis_container,
    solace_container,
    solace_names,
):
    """Builder for app Settings wired to every container plus this test's own
    Solace topic/queue; keyword overrides are passed through to Settings."""

    def build(**overrides) -> Settings:
        return app_settings(
            postgres_container,
            clickhouse_container,
            redis_container,
            **{**solace_overrides(solace_container, solace_names), **overrides},
        )

    return build


@pytest.fixture
def solace_publisher(solace_container, solace_names):
    publisher = SolacePublisher(
        host=solace_container.get_container_host_ip(),
        port=int(solace_container.get_exposed_port(55555)),
        vpn="default",
        username="admin",
        password="admin",
        topic=solace_names.topic,
    )
    yield publisher
    publisher.close()


@pytest.fixture
def cid_caplog(caplog):
    """caplog with the production CorrelationIdFilter attached, so captured
    records expose `record.correlation_id`. Real logging, no mocking."""
    from core.correlation import CorrelationIdFilter

    caplog.handler.addFilter(CorrelationIdFilter())
    return caplog
