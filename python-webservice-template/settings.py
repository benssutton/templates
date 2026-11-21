import json
from functools import lru_cache
from typing import Annotated, Literal

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_title: str = "Template Fast API Project"
    app_version: str = "1.0.0"
    app_description: str = (
        "A Python FastAPI service with MCP endpoints, ready for Claude"
    )

    status: str = "running"

    server_host: str = "0.0.0.0"
    server_port: int = 8443
    ssl_keyfile: str = "./certs/key.pem"
    ssl_certfile: str = "./certs/cert.pem"
    # Seconds uvicorn waits for in-flight requests on SIGTERM before closing them.
    shutdown_timeout_seconds: float = Field(default=30.0, gt=0)

    mcp_name: str = "python-template"
    mcp_instructions: str = "Tools for this template application."
    # /mcp DNS-rebinding protection: allowed Host header values (Origin is checked
    # against the same hosts). An entry without a port matches any port. List every
    # public host name per deployment. Must not be empty.
    # The env var may be a JSON array or a comma-separated list.
    mcp_allowed_hosts: Annotated[list[str], NoDecode] = Field(
        default=["localhost"], min_length=1
    )

    @field_validator("mcp_allowed_hosts", mode="before")
    @classmethod
    def _split_hosts(cls, value: object) -> object:
        if not isinstance(value, str):
            return value
        if value.lstrip().startswith("["):
            return json.loads(value)
        return [host.strip() for host in value.split(",") if host.strip()]

    # Same user/password/db as docker-compose and .env.example.
    postgres_url: SecretStr = SecretStr(
        "postgresql://user:password@localhost:5432/appdb"
    )
    postgres_pool_min_size: int = Field(default=2, ge=0)
    postgres_pool_max_size: int = Field(default=10, ge=1)
    # Every store call is bounded, so an outage answers 503 instead of hanging.
    postgres_command_timeout_seconds: float = Field(default=10.0, gt=0)
    postgres_acquire_timeout_seconds: float = Field(default=5.0, gt=0)

    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: SecretStr = SecretStr("")
    clickhouse_database: str = "default"
    clickhouse_connect_timeout_seconds: float = Field(default=5.0, gt=0)
    clickhouse_send_receive_timeout_seconds: float = Field(default=30.0, gt=0)

    redis_url: SecretStr = SecretStr("redis://localhost:6379/0")
    redis_socket_timeout_seconds: float = Field(default=5.0, gt=0)
    redis_connect_timeout_seconds: float = Field(default=5.0, gt=0)
    redis_max_connections: int = Field(default=50, ge=1)

    # Memtable -> run threshold.
    lsm_flush_rows: int = Field(default=1000, gt=0)
    # Run count -> compaction threshold (one run has nothing to merge).
    lsm_compaction_runs: int = Field(default=4, ge=2)
    lsm_key_columns: list[str] = ["id"]  # merge partition key; single extension point

    # Ingestion transport selector
    ingest_transport: Literal["solace", "noop"] = "noop"

    # Connection retry/backoff (Postgres, Redis, ClickHouse, Solace startup)
    connect_max_attempts: int = Field(default=5, ge=1)
    connect_base_delay: float = Field(default=1.0, ge=0)
    connect_max_delay: float = Field(default=30.0, ge=0)

    # Observability
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    # OTLP/HTTP collector base URL (e.g. http://otel-collector:4318); None = no tracing.
    otel_exporter_otlp_endpoint: str | None = None
    otel_service_name: str = "python-template"
    metrics_enabled: bool = True
    # Per-dependency ping timeout.
    health_check_timeout_seconds: float = Field(default=2.0, gt=0)
    # Dependency probe results are reused for this long, so frequent scrapes of
    # /metrics and /health/* do not ping every store each time. 0 disables.
    health_cache_ttl_seconds: float = Field(default=5.0, ge=0)
    # None = staleness never reported.
    ingest_staleness_threshold_seconds: float | None = None
    # Stale -> 503 only if True.
    ingest_stale_fails_readiness: bool = False
    # Non-CONNECTED longer than this -> SIGTERM; None disables the watchdog.
    ingest_max_disconnect_seconds: float | None = 60.0
    # Streaming ingest loop: transport and settlement backoff, give-up point.
    ingest_retry_base_delay: float = Field(default=1.0, gt=0)
    ingest_retry_max_delay: float = Field(default=60.0, gt=0)
    # 0 disables the failure-count shutdown
    ingest_max_consecutive_failures: int = Field(default=5, ge=0)
    ingest_join_timeout_seconds: float = Field(default=10.0, gt=0)

    # Inbound size limits
    # Over-limit HTTP body -> 413.
    max_request_body_bytes: int = 16 * 1024 * 1024
    # Over-limit stream batch -> dropped+logged.
    max_ingest_batch_bytes: int = 16 * 1024 * 1024

    # Correlation ID header name (inbound adoption + response echo).
    correlation_id_header: str = "X-Request-ID"

    # CORS — permissive by default for local UI dev; tighten per deployment.
    cors_allow_origins: list[str] = ["*"]
    cors_allow_methods: list[str] = ["*"]
    cors_allow_headers: list[str] = ["*"]
    cors_allow_credentials: bool = False  # must stay False while origins == ["*"]

    # Bearer-token (JWT, HS256) authentication. Off by default; when on, every
    # route including /mcp needs a valid token except /health/live and /health/ready.
    auth_enabled: bool = False
    auth_jwt_secret: SecretStr | None = None
    auth_jwt_issuer: str | None = None
    auth_jwt_audience: str | None = None

    @model_validator(mode="after")
    def _retry_delays_ordered(self) -> "Settings":
        if self.ingest_retry_max_delay < self.ingest_retry_base_delay:
            raise ValueError(
                "ingest_retry_max_delay must be >= ingest_retry_base_delay"
            )
        return self

    @model_validator(mode="after")
    def _auth_needs_secret(self) -> "Settings":
        if self.auth_enabled and not (
            self.auth_jwt_secret and self.auth_jwt_secret.get_secret_value()
        ):
            raise ValueError("auth_enabled=True requires auth_jwt_secret")
        return self

    @model_validator(mode="after")
    def _cors_credentials_check(self) -> "Settings":
        if self.cors_allow_credentials and "*" in self.cors_allow_origins:
            raise ValueError(
                "cors_allow_credentials=True is incompatible with cors_allow_origins=['*']. "
                "Specify explicit origins when enabling credentials."
            )
        return self

    # Solace PubSub+ (Guaranteed messaging; used when ingest_transport="solace")
    solace_host: str = "localhost"
    solace_port: int = 55555
    solace_vpn: str = "default"
    solace_username: str = "admin"
    solace_password: SecretStr = SecretStr("admin")
    solace_topic: str = "ingest/batches"
    # Durable exclusive queue this instance binds to (created on start if the
    # client profile allows). ONE QUEUE PER REPLICA: the LSM store is a
    # per-instance cache, and an exclusive queue delivers to one consumer only.
    solace_queue: str = "python-template/ingest"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
