# java-webservice-template

A Micronaut (Java 25) service that mirrors `python-webservice-template`
feature-for-feature, built so a developer can read either codebase and map one
onto the other. Same endpoints, same request/response shapes, minimal business
logic — a reusable foundation wired to common technologies.

Design + Python↔Java mapping: `../docs/superpowers/specs/2026-06-15-java-webservice-template-design.md`.

## Stack
- **Micronaut 5** on **Java 25** (Temurin), **Maven**
- **Virtual threads** — controllers run blocking handlers via `@ExecuteOn(TaskExecutors.BLOCKING)`
- **Micronaut Data JDBC** + HikariCP (Postgres; pgvector/pgvector image; `vector` extension enabled by the init script), **clickhouse-jdbc** (ClickHouse), **Jedis** + RedisJSON
- **Apache Arrow** + **Solace PubSub+ Guaranteed messaging** stream ingestion
- **Micronaut Serialization**, Jakarta Validation, **Micrometer/Prometheus**, OpenAPI/Swagger-UI
- **JUnit 5 + Micronaut Test + Testcontainers** (real dependencies, no mocks)

## Feature map (entry points)

| Feature | Entry point |
|---|---|
| App bootstrap (the `create_app`/DI analogue) | [Application.java](src/main/java/com/example/template/Application.java) |
| Typed settings | [config/AppSettings.java](src/main/java/com/example/template/config/AppSettings.java) + [application.yml](src/main/resources/application.yml) |
| `/config` (Postgres, Micronaut Data) | [controller/ConfigController.java](src/main/java/com/example/template/controller/ConfigController.java) |
| `/data` (ClickHouse) | [controller/DataController.java](src/main/java/com/example/template/controller/DataController.java) |
| `/data/cache` + `/data/ingest` (LSM) | [controller/DataController.java](src/main/java/com/example/template/controller/DataController.java) |
| `/cache` (Redis + RedisJSON) | [controller/CacheController.java](src/main/java/com/example/template/controller/CacheController.java) |
| `/health/{live,ready,status}` | [controller/HealthController.java](src/main/java/com/example/template/controller/HealthController.java), [service/HealthService.java](src/main/java/com/example/template/service/HealthService.java) |
| `/metrics` (Prometheus) | [observability/MetricsController.java](src/main/java/com/example/template/observability/MetricsController.java) |
| Correlation-ID (MDC propagation) | [core/CorrelationFilter.java](src/main/java/com/example/template/core/CorrelationFilter.java) |
| Server-Timing attribution | [core/ServerTimingFilter.java](src/main/java/com/example/template/core/ServerTimingFilter.java), [core/Timed.java](src/main/java/com/example/template/core/Timed.java) |
| Simplified append-only LSM store | [persistence/streamstore/LsmStore.java](src/main/java/com/example/template/persistence/streamstore/LsmStore.java) |
| Stream ingestion (thread, backoff, watchdog) | [service/StreamIngestService.java](src/main/java/com/example/template/service/StreamIngestService.java) |
| Solace / noop transports + `Delivery` settlement | [ingestion/solace/SolaceBatchConsumer.java](src/main/java/com/example/template/ingestion/solace/SolaceBatchConsumer.java), [ingestion/NoopBatchConsumer.java](src/main/java/com/example/template/ingestion/NoopBatchConsumer.java), [ingestion/Delivery.java](src/main/java/com/example/template/ingestion/Delivery.java) |
| Ingest settings | [config/IngestSettings.java](src/main/java/com/example/template/config/IngestSettings.java) |
| Ingest lifecycle wiring (graceful shutdown, `System.exit(3)` hook) | [service/StreamIngestBeans.java](src/main/java/com/example/template/service/StreamIngestBeans.java) |
| Demo publisher + Arrow IPC encoder | [publisher/ExampleSolacePublisher.java](src/main/java/com/example/template/publisher/ExampleSolacePublisher.java), [publisher/ArrowBatches.java](src/main/java/com/example/template/publisher/ArrowBatches.java) |
| Fail-fast startup smoke-test | [core/StartupSmoke.java](src/main/java/com/example/template/core/StartupSmoke.java) |
| Connect-with-backoff | [core/Retry.java](src/main/java/com/example/template/core/Retry.java) |
| Shared Testcontainers base | [test/.../support/IntegrationSupport.java](src/test/java/com/example/template/support/IntegrationSupport.java) |
| Solace test broker (one per JVM) + SEMP assertions | [test/.../support/SolaceSupport.java](src/test/java/com/example/template/support/SolaceSupport.java), [test/.../support/Semp.java](src/test/java/com/example/template/support/Semp.java) |
| k6 performance tests | [tests/performance/](tests/performance/) |

## Build & test
```bash
cd java-webservice-template
# JAVA_HOME must point at a JDK 25; Docker must be running (Testcontainers).
./mvnw verify      # runs all tests + JaCoCo coverage gate
```
On Windows + Docker Desktop, the `windows-docker` Maven profile auto-activates to
pin `DOCKER_HOST`; on Linux/CI it stays inactive.

Tests start one Solace broker per JVM (`SolaceSupport`, about a minute to come
up) and isolate by unique topic/queue names; `Semp` asserts broker-side state
(queue existence, subscriptions, message, bind and unacked counts) and network partitions are simulated with `docker pause`.
The embedded server runs on a random port (`src/test/resources/application-test.yml`)
because the broker's SEMP API uses 8080.

## Streaming ingestion (Solace)

A dedicated ingest thread consumes Arrow IPC batches from Solace Guaranteed
messaging and writes them into the LSM store, served from `/data/cache`.

- **Transport:** `template.ingest.transport` (env `INGEST_TRANSPORT`) defaults to
  `solace`. `SolaceBatchConsumer` binds a **durable exclusive queue**
  (`template.ingest.solace.queue`, default `java-template/ingest`), created on
  start (`CREATE_ON_START`) with the topic subscription
  (`template.ingest.solace.topic`, default `ingest/batches`) attached, using client
  acknowledgement. One queue per replica: the LSM store is a per-instance cache.
- **Settle after write.** Each message becomes a [`Delivery`](src/main/java/com/example/template/ingestion/Delivery.java);
  `StreamIngestService` decides and settles it exactly once:

  | Situation | Outcome |
  |---|---|
  | Written to the store | ACCEPTED (acked) |
  | Store write raised | FAILED: redelivered, after a jittered, doubling settlement backoff (1 s up to 60 s) |
  | Not Arrow IPC / unmergeable / over `max-batch-bytes` | REJECTED: never redelivered (DMQ if configured) |

  Auto-created queues use Solace's default unlimited max-redelivery and have no
  dead-message queue; production queues should configure both.
- **`noop` opt-out:** `INGEST_TRANSPORT=noop` connects to nothing and idles, for
  HTTP-only ingest (`POST /data/ingest`) without a broker. Under compose, `app`
  depends on a healthy `solace`, so skip that dependency explicitly:
  `docker compose up -d postgres clickhouse redis && INGEST_TRANSPORT=noop docker compose up -d --no-deps app`.
- **Extension point: Message Replay.** The durable queue buffers only unacked
  messages; to rebuild the in-memory cache after a restart, build the receiver in
  `SolaceBatchConsumer` with a replay strategy
  (`withMessageReplay(ReplayStrategy...)`) against a broker replay log, configured
  alongside `template.ingest.solace.queue` / `topic`.
- **Connect timing (differs from Python):** the consumer connects lazily inside the
  ingest loop; each `consume()` builds a fresh `MessagingService`. With the broker
  unreachable the app still comes up live with ingest `DOWN`, and the retry
  failure path or the disconnect watchdog exits the process. Python connects with
  backoff in its lifespan and fails startup instead. Both reconnect with a fresh
  service after a terminal interruption.
- **Batch size limit (differs from Python):** `max-batch-bytes` is checked against
  the encoded message payload, all-or-nothing, before any write. Python checks the
  decoded Arrow `nbytes` of each batch, also before any write.
- **Resilience and shutdown:** SDK lifecycle listeners drive `ConnectionState`;
  the SDK reconnects forever. Transport failures retry with jittered exponential
  backoff; a successful bind (the persistent receiver started) or a delivery resets
  the backoff and the failure count. 5 consecutive failures without a successful
  bind, or the transport staying disconnected longer than
  `max-disconnect-seconds`, trigger the shutdown hook, which calls `System.exit(3)`
  on a separate non-blocking thread so an orchestrator restarts the process.
  Graceful shutdown uses `@Bean(preDestroy = "close")` in `StreamIngestBeans`
  (Micronaut 5 does not infer `close()` on factory beans), which stops the ingest
  thread and closes the Solace session.
- **Config** (`template.ingest.*`, see [IngestSettings.java](src/main/java/com/example/template/config/IngestSettings.java)
  and [application.yml](src/main/resources/application.yml)): `transport`
  (`INGEST_TRANSPORT`), `max-batch-bytes`, `max-disconnect-seconds`,
  `staleness-threshold-seconds`, and `solace.host|port|vpn|username|password|topic|queue`
  (env `SOLACE_HOST`, `SOLACE_PORT`, `SOLACE_VPN`, `SOLACE_USERNAME`,
  `SOLACE_PASSWORD`, `SOLACE_TOPIC`, `SOLACE_QUEUE`).

## Run the full stack (HTTPS on 443)
```bash
docker compose up -d --build --wait
curl -fsk https://localhost/health/ready
# Swagger UI: https://localhost/swagger-ui  •  MCP: https://localhost/mcp
```
Compose also starts the `solace` broker (SMF on 55555, SEMP on 8080) and a
`solace-publisher` demo that runs `ExampleSolacePublisher`, continuously
publishing Arrow batches to `ingest/batches`. The publisher has
`restart: on-failure` because the broker's health check passes shortly before the
message VPN accepts clients and the demo does not retry its connect. To run the
app without the broker (HTTP-only ingest), start its other dependencies first and
skip `app`'s `solace` dependency with `--no-deps`; `INGEST_TRANSPORT=noop docker compose up app`
alone would still start the broker and wait for it to be healthy:
```bash
docker compose up -d postgres clickhouse redis
INGEST_TRANSPORT=noop docker compose up -d --no-deps app
```

## How this maps to python-webservice-template
Micronaut's `ApplicationContext` replaces the Python hand-rolled DI container;
`@MicronautTest` replaces `create_app` isolation. Deliberate divergences:
native DI; **append-only LSM store** (compaction pushed to the client, so
`/data/cache` rows carry `seqno`+`op`); **Jedis** for RedisJSON; **async-profiler**
for Layer-2 profiling; lazy Solace connect and an encoded-payload batch size check
(see [Streaming ingestion](#streaming-ingestion-solace)). Everything else mirrors the Python structure and naming.

## Performance profiling
See [PERF_PROFILING_RUNBOOK.md](PERF_PROFILING_RUNBOOK.md) — Layer 1 (k6 +
Server-Timing) and Layer 2 (async-profiler).
