# Python Template - Getting Started Guide

## Prerequisites
- Docker Desktop running
- Python 3.12+

## Getting Set Up

### 1. Create Local Docker Services 

```bash
# Start all services (Postgres, Clickhouse, Redis & Flight Server)
docker compose up -d --build

# Verify all containers are running
docker compose ps
```

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3. Generate Self-signed SSL Certificates for HTTPS

```bash
python ./certs/generate_self_signed_cert.py
```

## Running the Application

```bash
python main.py
```

Once running, swagger documentation will be available at https://localhost/docs#/

Both run modes serve HTTPS on the default port 443 with the self-signed dev cert:
`python main.py` and the `app` container started by `docker compose up` both expose
`https://localhost/`. Your browser will warn about the self-signed certificate —
accept it to proceed (dev only).

## Running the Tests

To run the functional pytests with a coverage report at htmlcov/index.html:
```bash
pytest tests/ -v --cov --cov-report=html
```

To run the smoke tests:

First, make sure the k6 image is built:
```bash
docker build -t perf-scripts ./tests/performance
```

```bash
docker run --rm --network python-template_default -e BASE_URL=https://app perf-scripts run /scripts/smoke.js
```

To run the performance tests:
```bash
docker run --rm --network python-template_default -e BASE_URL=https://app perf-scripts run /scripts/load.js
```

To run the stress tests:
```bash
docker run --rm --network python-template_default -e BASE_URL=https://app -e TARGET_RPS=50 perf-scripts run /scripts/stress.js
```

## Integrating with Claude Desktop

Start the FastAPI app — /mcp is mounted at https://localhost/mcp.
Add to Claude Desktop's claude_desktop_config.json. Because the dev cert is
self-signed, mcp-remote must be told to accept it via NODE_TLS_REJECT_UNAUTHORIZED=0
(dev only — do not use against production endpoints):

  "mcpServers": {
    "python-template": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "https://localhost/mcp"],
      "env": { "NODE_TLS_REJECT_UNAUTHORIZED": "0" }
    }
  },

Restart Claude Desktop. The health tool should appear.

