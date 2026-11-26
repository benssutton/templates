# Templates

This repo is intended to serve as a template for services in Python, Rust, Java and TypeScript that serve as solid foundations for development with agentic AI.  The templates purposefully do not contain business logic, but do contain wiring to common technologies, implement fully automated tests with coverage > 90% and aim to demonstrate simple and transparent patterns.  The objectives include:

1. Provide proven, reusable boilerplate that covers common technologies.  There is little value spending time or tokens re-creating boilerplate for new applications.  
2. Demonstrate best-in-class design for testability.  Faithful and comprehensive test coverage is a critical guardrail when using AI.
3. Demonstrate enterprise-level robustness and stability through self-recovery and exposure of observability data points.
4. Serve as a reference for teams (and AI agents) to converge on.  Recognisable patterns facilitate understanding.
5. Separate concerns between modules.  Decoupling infrastructure from domain logic, and further decoupling sub-domains delineates and improves understanding of scope.
6. Use idiomatic code wherever possible; and transparent reasoning where not.
7. Through all of the above assist code reviewers pattern-match against what good looks like.
8. Serve as a light-weight educational/debugging tool that developers can easily step through.


# Architectural Decisions

Testing
1. Testing should use real services/dependencies wherever possible.  Avoid mocks and patches.
2. Testing should exercise real endpoints (HTTP, websockets)
3. Tests should be async and run in parallel where possible to shorten the test cycle (Python: pytest-xdist, 2 workers; Java: JUnit class-level concurrency; Rust: cargo test's default).
4. Tests should include performance benchmarks
5. Tests should be runnable locally as well as in the build pipeline.  Results of tests should be used as control gates in the CI/CD process.

Languages
1. Compiler support should be leveraged wherever possible.  Pylance and Pyright should be used to support applications in Python

APIs
1. All services should be accessed through REST endpoints: application management, configuration, observability and functional services should all be accessible via REST.
2. MCPs are optional.  MCP Tools should simply mirror REST endpoints.  

Compression
1. Responses are compressed by default.  Uncompressed responses are available via Accept-Encoding (identity).  COMPRESSION_ENABLED=false turns off compression.
2. ZSTD is the byte compression codec of choice as this gives the best balance of CPU/latency on compression (microseconds) and for payloads above ~1 KiB over a network it is quicker and requires less CPU for a client to receive a ZSTD payload and decompress it versus reading uncompressed data from the i/o.
3. GZip is not a speed-efficient compression codec and has been included only to support Prometheus via the /metrics endpoint.

Stream store (LSM)
1. Purpose: an in-memory cache of streamed rows, held as columnar Arrow so query engines (Polars, DataFusion) read it zero-copy, in as few contiguous parts as write amplification allows.
2. Supersede on write, not on read. A writer-private index maps each key to its live `(part, row)`. When a newer version arrives, the old row's bit in its part's `live` bitmap is cleared; a `delete` clears the bit and is not stored. Reads therefore never deduplicate, and merges never handle tombstones: they only filter out dead rows.
3. Parts are immutable Arrow `RecordBatch`es sorted by key, each with a copy-on-write `live` bitmap and a live-row count. Each incoming batch, whatever its size, becomes one L0 part. A part with no live rows left is dropped.
4. Layers: `L` layers with `P` parts per layer (`LSM_LAYERS=4`, `LSM_PARTS_PER_LAYER=8`). When layer `i < L-1` holds `P` parts, they are merged (live rows only, sorted by key) into one part appended to layer `i+1`. The top layer is never merged: once it holds `P` parts it keeps accepting more, so overflow happens only there.
5. Reads: engines get each part plus a `_live` column built from its bitmap (zero-copy) and filter on it. `GET /data/cache` k-way merges the sorted parts, skipping dead rows and stopping at `limit`; `total` is the maintained live count.
6. Capacity: `LSM_MAX_ROWS` caps the rows held, live plus the dead rows still inside parts. A message is admitted whole: if the rows it would store (its upserts after the newest-per-key dedup, counted before the rows it supersedes are freed) would take the rows held over the cap, none of it is written and the store freezes. From then on every valid non-empty message is refused, deletes included, so the store goes stale rather than partly updated.
   - Effects: HTTP answers 507; the stream rejects the message (never redelivered, never counted toward the consecutive-failure shutdown). Invalid or oversized batches are still 422 or 413. Every refusal is logged at ERROR as `FATAL:` with `fatal=true`. Reads keep serving everything received to date, readiness stays up, and the status page and metrics report `full`. Only rebuilding the store (a restart, or a refresh from a persistent store) unfreezes it.
7. Bounds, with batches counted as they arrive (`b̄` = mean rows per batch):
   - A part in layer `i` covers at least `P^i` consecutive batches, more when an all-delete batch adds no part or a dead part is dropped (about `P^i·b̄` rows, fewer once dead rows are dropped), so with P=8, L=4 a top-layer part covers at least 512 batches.
   - Parts below the top layer: at most `(L-1)(P-1)` = 21.
   - Top-layer parts after `n` batches: at most `floor(n / P^(L-1))`; it reaches `P` at `n = P^L` = 4,096 batches and then grows by at most one part every 512 batches. Total parts are at most `(L-1)(P-1) + floor(n / P^(L-1))`, which is 28 until the top layer overflows.
   - Write amplification: at most `L` = 4 (the initial write plus one rewrite per merge into layers 1 to L-1); top-layer parts are never rewritten.
8. Consequence of an unmerged top layer: superseded rows in a top-layer part stay in memory, and count toward `LSM_MAX_ROWS`, until every row of that part is dead or the store is rebuilt. Size `L` and `P` so that `P^L` batches covers the expected volume between rebuilds, or accept more top-layer parts beyond it.
9. Sizing: before choosing the in-memory store, weigh the frequency and size of batches, how often the store is rebuilt or refreshed from a persistent store (only that unfreezes a full store), and the RAM for the rows held plus merge headroom. A held row costs its Arrow data plus its encoded key and, while live, a key-index entry. A merge copies the live rows of a full layer into a new part before the old parts are freed; the largest, layer L-2 into the top, covers up to `P^(L-1)` = 512 batches by default, so peak memory is about the rows held plus 512 batches' worth of rows.

Resilience
1. Critical dependencies should be eagerly loaded and smoke-tested on application start (Fail Fast).  Disconnects should be re-tried with exponential back-off.  After a specified number of re-try failures, the application should shut down.

Observability
1. The application should expose endpoints that support optimal monitoring and observability

Security
1. Each template ships a **bearer-JWT seam that is wired but off** (`AUTH_ENABLED=false`). Turned on (`AUTH_ENABLED=true`, `AUTH_JWT_SECRET`, optional `AUTH_JWT_ISSUER` / `AUTH_JWT_AUDIENCE`; HS256), every route including `/mcp` needs `Authorization: Bearer <jwt>` except `/health/live` and `/health/ready`, and failures are 401 `{"detail": "Not authenticated"}`. Startup fails if it is on without a secret. Authorisation (roles, scopes) is left to the project.
2. CORS defaults to `*` for local development. Set explicit origins per deployment.
3. What the templates also enforce: request body size limits (413, counted after decompression, so a decompression bomb is refused), an `/mcp` Host/Origin allow-list (`MCP_ALLOWED_HOSTS`, a JSON array or comma-separated) against DNS rebinding, secrets that never appear in logs or health payloads, non-root container images listening on 8443, and dev TLS material generated by compose rather than baked into images.

Contract
1. The three templates are each idiomatic for their framework; they agree on the HTTP contract, not on code shape: paths, snake_case JSON, `{"detail": ...}` for every error (400 malformed, 401, 403, 404, 405, 406, 413, 415, 422, 500, 503), store outages as 503, `X-Request-ID` adopted only when it is 1-128 chars of `[A-Za-z0-9._-]`, and the same environment variable names.
2. The contract is pinned by `tests/performance/contract.js`, identical in all three services (`tests/cicd/test_perf_scripts.py`) and run by each service's `k6-smoke` step against its compose stack. It covers the HTTP surface above, the merged `/data/cache` view, the MCP tool names and their REST-identical results, and the shared metric names. Env var names are pinned by `tests/cicd/test_env_contract.py` against each service's `.env.example`. Each service's own suite pins what needs a configured or broken stack (401, 500, 503).
3. Stream ingest defaults to `INGEST_TRANSPORT=noop` (HTTP ingest only); Solace is opt-in, and the compose stacks opt in for the demo publisher.
4. Content coding is negotiated, never forced: a text-based response of at least `COMPRESSION_MIN_BYTES` (1024) is sent as zstd when `Accept-Encoding` allows it, else gzip, else uncompressed (a missing header or `identity` means uncompressed); event streams are never compressed, and text-based responses carry `Vary: Accept-Encoding`. Request bodies may be zstd or gzip, and `MAX_REQUEST_BODY_BYTES` counts their decompressed size; any other `Content-Encoding` is 415 with `Accept-Encoding: zstd, gzip`. `COMPRESSION_ZSTD_LEVEL` (3, 1-19) is server-side only. `COMPRESSION_REQUIRE_ZSTD=true` answers 406 to clients that cannot accept zstd, except on `/health/live`, `/health/ready`, `/metrics`, `/mcp` and CORS preflight.


# Assumed Use Cases & Technologies

## Languages
- Python: exceptionally, where access to analytic, statistical and ML libraries are the key priority
- Rust: exceptionally, for high-CPU or concurrent workloads where use of parallelism and SIMD vectors are able to make a material difference to processing time.
- Typescript: for UIs
- Java: everything else.  The default language.


# Current Progress

The Python Webservice under python-webservice-template is ready for use.

The Java Webservice under java-webservice-template is ready for use — Micronaut 5
on Java 25, with Flyway migrations, Spotless formatting and a 90% JaCoCo gate.

The Rust Webservice under rust-webservice-template is ready for use — axum on a
tower-http layer stack, with sqlx migrations, OpenAPI generated from the routes
(utoipa-axum) and cargo-deny. Its LSM stream store runs on arrow-rs compute
kernels (with a criterion benchmark), and Solace is consumed over AMQP 1.0 behind
the `solace` Cargo feature.

All three share the HTTP contract above, migrate their own Postgres and ClickHouse
schemas at startup, log JSON with a configurable level, and export OpenTelemetry
traces when an OTLP endpoint is configured.

Other templates that might show in this repo are still works-in-progress.

# Pipeline

One entry point, identical locally and in GitHub Actions: `python run_CICD.py` (use the p315 conda env).

| Command | What it does |
|---|---|
| `run_CICD.py list` | services and whether they are ready or wip |
| `run_CICD.py changed [--base origin/main]` | services affected by your changes |
| `run_CICD.py changed fast` / `check` / `ci` | run a step set on changed services only |
| `run_CICD.py python\|java\|rust <step...>` | one service (`fmt`, `fmt-check`, `lint`, `typecheck`, `test`, `deny`, `build-image`, `scan-image`, `k6-smoke`, `k6-load`, ...) |
| `run_CICD.py security [trivy\|osv\|gitleaks\|semgrep]` | repo-wide scanners (Docker) |
| `run_CICD.py sonar <service>` | local SonarQube analysis (see sonar/README.md) |

Step sets: **fast** (format + lint, seconds) < **check** (+ tests/coverage) < **ci** (+ image build/scan, k6 smoke and the shared contract; Rust also runs cargo-deny).

**Change detection** has one source of truth: `cicd/services.py` + `cicd/changed.py`. Changes under a service directory run that service; changes to `run_CICD.py`, `cicd/`, `.github/`, scanner configs or `requirements-dev.txt` run all; markdown/docs-only changes run no service jobs (CI still starts, so `Pipeline Status` reports and passes).

**GitHub**: `ci.yml` runs on every push to main, every pull request (no `paths-ignore`) and manual dispatch (input `services`: `changed`|`all`); the `pipeline-tests` job always runs `tests/cicd`. If the push base is unknown (force-push) it diffs against `HEAD~1`, and with no usable base it runs all services. There are no scheduled workflows; Dependabot (monthly, all ecosystems) is the only recurring automation. The `Pipeline Status` job is the single required check for branch protection (Settings > Branches > require status check `Pipeline Status`). Python, Java and Rust are all `ready=True` and gate `Pipeline Status`. A new service starts as `ready=False` (it runs but never blocks, and its job has `continue-on-error: true`). To promote it, set `ready=True` in `cicd/services.py`, add it to `status.needs` in `ci.yml` and remove `continue-on-error` from its workflow; `tests/cicd/test_workflows.py` checks that all three are done together.

**Local hooks**: `.pre-commit-config.yaml` runs gitleaks on commit and `changed fast` on push (`pre-commit install --install-hooks && pre-commit install --hook-type pre-push`).

**Suppressions** (`.trivyignore`, `osv-scanner.toml`, `.gitleaks.toml`, `sonar/false-positives.md`) must carry a justification for every entry.

## Former GitLab CI jobs

The per-service `.gitlab-ci.yml` files were retired; their jobs map as follows.

| GitLab job | Now |
|---|---|
| Python `pytest` | `run_CICD.py python test` |
| Java `build-test` | `run_CICD.py java check` / `ci` |
| `k6-smoke` | `k6-smoke` step (part of `ci`) |
| `k6-load` | local only: `run_CICD.py <svc> k6-load` |
| `k6-stress` | no pipeline step; run locally with the service's `docker compose up -d --wait`, then `tests/performance/stress.js` via the k6 image (`-e BASE_URL=... -e TARGET_RPS=50`) |

## Known limitations

**Baselined findings.** The first scan's findings are suppressed with time-boxed entries: `.trivyignore` (`exp:`), `osv-scanner.toml` (`ignoreUntil`). Most expire on **2026-12-14**, after which the pipeline fails until the follow-up dependency/base-image bump PR lands: jackson, netty, guava, postgresql, micrometer, httpcore5, black, cryptography, Debian base-image CVEs and astral-tokio-tar. (The `# nosemgrep` missing-user baselines are gone: every service image now runs as uid 10001.) The not-compiled rmcp advisories and the filelock false positives (`osv-scanner.toml`) expire on **2027-02-14** instead. The Rust crates.io entries were re-triaged when Rust was promoted; each names the crate that pulls it in.

**Scanner coverage.**

| Scanner | Limit |
|---|---|
| OSV | Maven is scanned via a CycloneDX SBOM generated in Docker |
| Dependabot | for the Python service it bumps `requirements.in` and recompiles the `requirements.txt` lock (pip-compile); check the result targets Linux / Python 3.12 |
| Trivy | cannot see BOM-managed Maven versions |
| gitleaks / semgrep | need the repo's `.git` directory: in a git worktree (`.git` is a file) they fail fast, so run `security` from the main checkout or a clone |

**Local SonarQube** shares its Docker Compose project name (`sonar`) and volumes with turbo-parakeet's `sonar/compose.yml`: one local SonarQube serves both repos. See sonar/README.md for token setup.

**Not verifiable until the first GitHub run:** CodeQL for Rust, `upload-sarif` (needs code scanning enabled on the repo), and the Docker steps on ubuntu runners. Manual step: require `Pipeline Status` in branch protection.
