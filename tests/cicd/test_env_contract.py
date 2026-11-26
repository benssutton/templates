"""Every template reads the same environment variable names.

Each service lists every variable it reads in its .env.example. This test checks
that each list holds the shared set, and that each listed name is really read by
the service's settings, so the example cannot drift from the code.
"""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

SHARED = frozenset("""
APP_TITLE APP_VERSION APP_DESCRIPTION SERVER_HOST SERVER_PORT LOG_LEVEL
CORRELATION_ID_HEADER METRICS_ENABLED OTEL_SERVICE_NAME OTEL_EXPORTER_OTLP_ENDPOINT
AUTH_ENABLED AUTH_JWT_SECRET AUTH_JWT_ISSUER AUTH_JWT_AUDIENCE
CORS_ALLOW_ORIGINS CORS_ALLOW_METHODS CORS_ALLOW_HEADERS CORS_ALLOW_CREDENTIALS
MCP_NAME MCP_INSTRUCTIONS MCP_ALLOWED_HOSTS
HEALTH_CHECK_TIMEOUT_SECONDS HEALTH_CACHE_TTL_SECONDS
CONNECT_MAX_ATTEMPTS CONNECT_BASE_DELAY CONNECT_MAX_DELAY
MAX_REQUEST_BODY_BYTES MAX_INGEST_BATCH_BYTES
COMPRESSION_ENABLED COMPRESSION_MIN_BYTES COMPRESSION_ZSTD_LEVEL COMPRESSION_REQUIRE_ZSTD
POSTGRES_URL POSTGRES_POOL_MIN_SIZE POSTGRES_POOL_MAX_SIZE POSTGRES_ACQUIRE_TIMEOUT_SECONDS
REDIS_URL
CLICKHOUSE_HOST CLICKHOUSE_PORT CLICKHOUSE_DATABASE CLICKHOUSE_USER CLICKHOUSE_PASSWORD
INGEST_TRANSPORT INGEST_MAX_DISCONNECT_SECONDS INGEST_MAX_CONSECUTIVE_FAILURES
INGEST_RETRY_BASE_DELAY INGEST_RETRY_MAX_DELAY
INGEST_STALENESS_THRESHOLD_SECONDS INGEST_STALE_FAILS_READINESS
LSM_LAYERS LSM_PARTS_PER_LAYER LSM_KEY_COLUMNS
SOLACE_HOST SOLACE_PORT SOLACE_VPN SOLACE_USERNAME SOLACE_PASSWORD SOLACE_TOPIC SOLACE_QUEUE
SSL_CERTFILE SSL_KEYFILE
""".split())

# Shared names a service cannot honour, each with the reason.
EXCEPTIONS = {
    "python": {
        "LSM_LAYERS": "pending the layered-store port",
        "LSM_PARTS_PER_LAYER": "pending the layered-store port",
    },
    "java": {
        "SSL_CERTFILE": "Micronaut serves TLS from a PKCS12 keystore (application-docker.yml)",
        "SSL_KEYFILE": "Micronaut serves TLS from a PKCS12 keystore (application-docker.yml)",
        "LSM_LAYERS": "pending the layered-store port",
        "LSM_PARTS_PER_LAYER": "pending the layered-store port",
    },
}


def _example(service: str) -> set[str]:
    text = (ROOT / f"{service}-webservice-template" / ".env.example").read_text()
    return {m.group(1) for m in re.finditer(r"^#?\s*([A-Z][A-Z0-9_]*)=", text, re.M)}


def _read_by(service: str) -> set[str]:
    d = ROOT / f"{service}-webservice-template"
    if service == "python":
        fields = re.findall(r"^    ([a-z][a-z0-9_]*)\s*:", (d / "settings.py").read_text(), re.M)
    elif service == "rust":
        fields = re.findall(r"^    pub ([a-z][a-z0-9_]*):", (d / "src/settings.rs").read_text(), re.M)
    else:
        yml = "".join(p.read_text() for p in (d / "src/main/resources").glob("application*.yml"))
        java = (d / "src/main/java/com/example/template/config/EnvContract.java").read_text()
        return set(re.findall(r"\$\{([A-Z][A-Z0-9_]*)", yml)) | set(re.findall(r'"([A-Z][A-Z0-9_]+)"', java))
    return {f.upper() for f in fields}


@pytest.mark.parametrize("service", ["python", "java", "rust"])
def test_env_example_lists_the_shared_names(service: str) -> None:
    missing = SHARED - _example(service) - set(EXCEPTIONS.get(service, {}))
    assert not missing, f"{service}/.env.example lacks shared names: {sorted(missing)}"


@pytest.mark.parametrize("service", ["python", "java", "rust"])
def test_every_listed_name_is_read(service: str) -> None:
    unread = _example(service) - _read_by(service)
    assert not unread, f"{service}/.env.example lists names the service never reads: {sorted(unread)}"
