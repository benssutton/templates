from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_title: str = "Template Fast API Project"
    app_version: str = "1.0.0"
    app_description: str = "A Python FAST API service with MCP endpoints and Rust extensions, ready for Claude"

    status: str = "running"

    server_host: str = "0.0.0.0"
    server_port: int = 443
    ssl_keyfile: str = "./certs/key.pem"
    ssl_certfile: str = "./certs/cert.pem"

    mcp_name: str = "python-template"
    mcp_instructions: str = "Tools for this template application."

    postgres_url: SecretStr = SecretStr("postgresql://appuser:password@localhost:5432/appdb")
    postgres_pool_min_size: int = 2
    postgres_pool_max_size: int = 10

    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: SecretStr = SecretStr("")
    clickhouse_database: str = "default"

    redis_url: SecretStr = SecretStr("redis://localhost:6379/0")

    flight_host: str = "localhost"
    flight_port: int = 8815          # pyarrow Flight default
    flight_ticket: str = "items"
    lsm_flush_rows: int = 1000       # memtable -> run threshold
    lsm_compaction_runs: int = 4     # run count -> compaction threshold
    lsm_key_columns: list[str] = ["id"]  # merge partition key; single extension point

    # Ingestion transport selector
    ingest_transport: Literal["flight", "solace"] = "flight"

    # Connection retry/backoff (Postgres, Redis, ClickHouse startup)
    connect_max_attempts: int = Field(5, ge=1)
    connect_base_delay: float = Field(1.0, ge=0)
    connect_max_delay: float = Field(30.0, ge=0)

    # Observability
    metrics_enabled: bool = True
    health_check_timeout_seconds: float = 2.0                 # per-dependency ping timeout
    ingest_staleness_threshold_seconds: float | None = None   # None = staleness never reported
    ingest_stale_fails_readiness: bool = False                # stale -> 503 only if True
    ingest_max_disconnect_seconds: float | None = 60.0        # non-CONNECTED longer than this -> SIGTERM; None disables

    # Solace — only resolved when ingest_transport="solace"
    solace_host: str = "localhost"
    solace_port: int = 55555
    solace_vpn: str = "default"
    solace_username: str = "admin"
    solace_password: SecretStr = SecretStr("admin")
    solace_topic: str = "ingest/batches"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
