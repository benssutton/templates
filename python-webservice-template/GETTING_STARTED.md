# Python Template - Getting Started Guide

## Prerequisites
- Docker Desktop running
- Python 3.12+

## Getting Set Up

### 1. Install Python dependencies

```bash
pip install -r requirements.txt -r requirements-dev.txt   # exact runtime pins + tests/linters
```

### 2. Generate the dev TLS certificate (only for `python main.py`)

```bash
python certs/generate_self_signed_cert.py   # writes certs/key.pem + certs/cert.pem (gitignored)
```

The compose stack needs no host-side step: its one-shot `certs` service writes a
self-signed certificate into a volume the `app` container mounts read-only.

### 3. Start the backing services

```bash
# Postgres/pgvector, ClickHouse, Redis, Solace + demo publisher, and the app
docker compose up -d --build --wait
docker compose ps
```

The compose stack streams from Solace (`INGEST_TRANSPORT=solace` is set on `app`);
Solace needs a Docker VM with at least 3 GiB RAM. The application default is
`noop` (HTTP ingest only), so for a broker-free stack start just the stores and
run the app locally:

```bash
docker compose up -d db clickhouse redis
python main.py
```

The app applies its Postgres and ClickHouse migrations (`migrations/`) at startup.

## Running the Application

```bash
python main.py
```

Swagger is at https://localhost:8443/docs when run with `python main.py` (HTTPS on
8443). Under compose the container listens on 8443 and publishes it as `443:8443`,
so it is reached at `https://localhost/`. Your browser will warn about the self-signed
certificate: accept it (dev only).

## Running the Tests

All tests start their own containers (Testcontainers); Docker must be running.

```bash
pytest tests/ -v --cov --cov-report=html    # coverage report at htmlcov/index.html
pytest tests/test_cache.py::test_post_cache_without_ttl -v   # one test
```

k6 runs against the compose stack from inside its network:

```bash
docker build -t perf-scripts ./tests/performance
docker run --rm --network python-template_default -e BASE_URL=https://app:8443 perf-scripts run /scripts/smoke.js
docker run --rm --network python-template_default -e BASE_URL=https://app:8443 perf-scripts run /scripts/load.js
docker run --rm --network python-template_default -e BASE_URL=https://app:8443 -e TARGET_RPS=50 perf-scripts run /scripts/stress.js
```

## Integrating with Claude Desktop

Start the app; `/mcp` is mounted at https://localhost/mcp. Add to Claude Desktop's
`claude_desktop_config.json`. Because the dev cert is self-signed, mcp-remote must
be told to accept it via `NODE_TLS_REJECT_UNAUTHORIZED=0` (dev only; never against
production endpoints):

```json
"mcpServers": {
  "python-template": {
    "command": "npx",
    "args": ["-y", "mcp-remote", "https://localhost/mcp"],
    "env": { "NODE_TLS_REJECT_UNAUTHORIZED": "0" }
  }
}
```

Restart Claude Desktop. The tools `get_health_status`, `list_config`,
`get_cache_entry`, `get_data` and `get_cached_data` should appear. With
`AUTH_ENABLED=true`, add `"--header", "Authorization: Bearer <jwt>"` to `args`.
