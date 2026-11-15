"""Build app Settings that point at the session's real test containers."""

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from pydantic import SecretStr

from settings import Settings


@dataclass(frozen=True)
class SolaceNames:
    """Per-test topic + queue: one shared broker, isolated by naming (the
    Solace multi-tenant idiom) instead of one broker per test."""

    topic: str
    queue: str


def new_solace_names() -> SolaceNames:
    uid = uuid4().hex[:12]
    return SolaceNames(topic=f"test/{uid}/batches", queue=f"test-{uid}")


def app_settings(pg, ch, redis, **overrides) -> Settings:
    """Settings for an isolated test app. Defaults to the `noop` transport;
    pass **solace_overrides(...) to bind a real Solace queue instead."""
    values: dict[str, Any] = dict(
        status="testing",
        postgres_url=SecretStr(
            f"postgresql://{pg.username}:{pg.password}@localhost:"
            f"{int(pg.get_exposed_port(5432))}/{pg.dbname}"
        ),
        clickhouse_host="localhost",
        clickhouse_port=int(ch.get_exposed_port(8123)),
        clickhouse_user=ch.username or "default",
        clickhouse_password=SecretStr(ch.password or ""),
        clickhouse_database="default",
        redis_url=SecretStr(f"redis://localhost:{int(redis.get_exposed_port(6379))}/0"),
        ingest_transport="noop",
        lsm_flush_rows=2,
        lsm_compaction_runs=2,
    )
    values.update(overrides)
    return Settings(**values)


def solace_overrides(solace, names: SolaceNames) -> dict[str, Any]:
    return dict(
        ingest_transport="solace",
        solace_host=solace.get_container_host_ip(),
        solace_port=int(solace.get_exposed_port(55555)),
        solace_vpn="default",
        solace_username="admin",
        solace_password=SecretStr("admin"),
        solace_topic=names.topic,
        solace_queue=names.queue,
    )
