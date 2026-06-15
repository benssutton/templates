# Java Webservice Template — Phase 5: MCP, Docker, CI, k6, Docs

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the template: MCP server at `/mcp` (one tool, mirroring Python), a self-signed-HTTPS Docker image on 443, the full docker-compose stack + profiling overlay, reused k6 performance scripts, GitLab CI with JUnit/JaCoCo/k6 gates, the JaCoCo coverage threshold, async-profiler runbook, and the README/CLAUDE.md that make the Python↔Java mapping navigable.

**Architecture:** MCP uses the official MCP Java SDK; because Micronaut is Netty-based and the SDK's HTTP transports are servlet-oriented, MCP integration is **spike-first** (Task 1) with a documented servlet-runtime fallback. The Docker image runs the packaged jar over HTTPS on 443 using a PKCS12 keystore generated at build time via keytool. docker-compose brings up Postgres, ClickHouse, redis-stack, a Java Flight server, and the app; the profiling overlay adds the flags async-profiler needs. k6 scripts are copied verbatim from the Python template (language-agnostic) and retargeted via `BASE_URL`.

**Tech Stack:** MCP Java SDK (`io.modelcontextprotocol.sdk:mcp`), keytool/PKCS12, Docker, docker-compose, k6 (grafana/k6 image), async-profiler, JaCoCo, GitLab CI.

**Reference:** `mcp_routers/tools.py`, `Dockerfile`, `docker-compose.yml`, `docker-compose.profiling.yml`, `tests/performance/*`, `.gitlab-ci.yml`, `certs/generate_self_signed_cert.py`, `PERF_PROFILING_RUNBOOK.md`, `README.md`, `CLAUDE.md`.

**Conventions:** as prior phases.

---

## File structure produced by this phase

| File | Responsibility |
|---|---|
| `pom.xml` | + MCP SDK; + JaCoCo plugin with coverage rule |
| `.../mcp/McpConfiguration.java` + `Tools.java` | MCP server + `get_health_status` tool |
| `.../mcp/McpController.java` (or servlet) | `/mcp` transport bridge |
| `certs/generate-keystore.sh` | keytool PKCS12 keystore generator |
| `src/main/resources/application-docker.yml` | HTTPS/443 + container hostnames |
| `Dockerfile` | temurin 25 build → HTTPS jar on 443 |
| `docker-compose.yml`, `docker-compose.profiling.yml` | full stack + profiling overlay |
| `.../flightserver/ExampleFlightServer.java` | standalone Flight server for compose |
| `tests/performance/**` | copied k6 scripts, retargeted |
| `tests/performance/profile/run_asyncprofiler.sh` | Layer-2 profiler |
| `.gitlab-ci.yml` | mvn verify + k6 gates |
| `README.md`, `CLAUDE.md`, `PERF_PROFILING_RUNBOOK.md` | docs |

---

## Task 1: MCP server at /mcp — SPIKE-first

**Files:**
- Modify: `pom.xml`
- Create: `.../mcp/McpConfiguration.java`, `.../mcp/Tools.java`, `.../mcp/McpController.java`
- Test: `.../mcp/McpEndpointTest.java`

> **Spike goal:** confirm how the MCP Java SDK's server transport bridges to Micronaut Netty. Two known paths: (a) the SDK's `HttpServletSseServerTransportProvider` under the `micronaut-servlet` runtime, or (b) a thin Micronaut `@Controller` exposing the SDK's streamable-HTTP/SSE handler. Start with (b); fall back to (a) if the controller bridge can't carry SSE cleanly. **Time-box the spike**; the acceptance test below defines done.

- [ ] **Step 1: Add the MCP SDK dependency**

```xml
<dependency><groupId>io.modelcontextprotocol.sdk</groupId><artifactId>mcp</artifactId><version>0.10.0</version><scope>compile</scope></dependency>
```
(Use the latest published MCP Java SDK version.)

- [ ] **Step 2: Write the tool registration** (mirrors `mcp_routers/tools.py`)

`.../mcp/Tools.java`:
```java
package com.example.template.mcp;

import com.example.template.service.HealthService;

/** Registers MCP tools. Mirrors the Python mcp_routers/tools.py single example
 *  tool: get_health_status returns the app's health status string. */
public final class Tools {
    private Tools() {}

    public static String getHealthStatus(HealthService health) {
        return health.detailedStatus().app().status();
    }
}
```

- [ ] **Step 3: Write the MCP server configuration + transport bridge**

`.../mcp/McpConfiguration.java` builds the SDK `McpServer` with the `get_health_status` tool. `.../mcp/McpController.java` exposes it at `/mcp`. The exact SDK builder API is the spike deliverable; the structure:
```java
package com.example.template.mcp;

import com.example.template.config.AppSettings;
import com.example.template.service.HealthService;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;

@Factory
public class McpConfiguration {
    // Spike: build io.modelcontextprotocol.server.McpServer with a synchronous tool
    // "get_health_status" whose handler calls Tools.getHealthStatus(healthService),
    // wired to a streamable-HTTP/SSE transport provider. Expose the provider as a bean
    // that McpController delegates to. Name/instructions come from AppSettings.
    @Singleton
    public McpServerHandle mcpServer(AppSettings settings, HealthService healthService) {
        // return new McpServerHandle(... built server + transport ...);
        throw new UnsupportedOperationException("Spike: wire MCP Java SDK server + transport");
    }
}
```
(Create a small `McpServerHandle` record/class holding the SDK server + transport handler the controller calls. Replace the `throw` once the SDK API is confirmed.)

- [ ] **Step 4: Write the acceptance test (the contract)**

`.../mcp/McpEndpointTest.java`. Asserts `/mcp` is mounted and the tool is discoverable. Minimal HTTP-level check (full MCP client handshake optional):
```java
package com.example.template.mcp;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@MicronautTest
class McpEndpointTest {

    @Inject @Client("/") HttpClient client;

    @Test
    void mcpEndpointIsMounted() {
        // The /mcp transport should respond (200 to an SSE/handshake GET, or 405/400
        // to a bare GET) — anything other than 404 proves it is mounted.
        int status;
        try {
            status = client.toBlocking().exchange(HttpRequest.GET("/mcp")).getStatus().getCode();
        } catch (HttpClientResponseException e) {
            status = e.getStatus().getCode();
        }
        assertThat(status).isNotEqualTo(HttpStatus.NOT_FOUND.getCode());
    }
}
```

- [ ] **Step 5: Iterate the spike until the test passes; commit**

```bash
cd c:/Users/Alexander/templates/java-webservice-template && ./mvnw -q test -Dtest=McpEndpointTest
cd c:/Users/Alexander/templates
git add java-webservice-template/pom.xml java-webservice-template/src/main/java/com/example/template/mcp java-webservice-template/src/test/java/com/example/template/mcp
git commit -m "feat(java): MCP server at /mcp with get_health_status tool"
```

> If the controller bridge cannot carry SSE, add `io.micronaut.servlet:micronaut-http-server-jetty` (or tomcat) as the runtime and mount the SDK's `HttpServletSseServerTransportProvider` servlet at `/mcp`. Document whichever path succeeded in `CLAUDE.md`.

---

## Task 2: Self-signed HTTPS + Docker image

**Files:**
- Create: `certs/generate-keystore.sh`, `src/main/resources/application-docker.yml`, `Dockerfile`, `.dockerignore`

- [ ] **Step 1: Write the keystore generator** (the `certs/generate_self_signed_cert.py` analogue)

`java-webservice-template/certs/generate-keystore.sh`:
```bash
#!/usr/bin/env bash
# Generate a self-signed PKCS12 keystore for local/dev HTTPS. SAN includes
# localhost + 'app' so in-network containers (Prometheus, k6) validate the host.
set -euo pipefail
OUT="${1:-certs/keystore.p12}"
PASS="${KEYSTORE_PASSWORD:-changeit}"
keytool -genkeypair -alias app -keyalg RSA -keysize 2048 -validity 3650 \
  -storetype PKCS12 -keystore "$OUT" -storepass "$PASS" \
  -dname "CN=localhost, OU=template, O=example, L=local, ST=local, C=US" \
  -ext "SAN=dns:localhost,dns:app,ip:127.0.0.1"
echo "Wrote $OUT"
```

- [ ] **Step 2: Write the docker profile config**

`src/main/resources/application-docker.yml` (activated by `MICRONAUT_ENVIRONMENTS=docker`):
```yaml
micronaut:
  server:
    port: 443
    ssl:
      enabled: true
      port: 443
      key-store:
        path: file:/app/certs/keystore.p12
        type: PKCS12
        password: ${KEYSTORE_PASSWORD:changeit}

datasources:
  default:
    url: jdbc:postgresql://postgres:5432/appdb
  clickhouse:
    url: jdbc:ch://clickhouse:8123/default

redis:
  uri: redis://redis:6379/0

template:
  ingest:
    flight:
      host: flight
```

- [ ] **Step 3: Write the Dockerfile** (multi-stage; temurin 25; HTTPS on 443)

`java-webservice-template/Dockerfile`:
```dockerfile
# syntax=docker/dockerfile:1
FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B -DskipTests package

FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=build /build/target/*.jar app.jar
COPY certs/generate-keystore.sh certs/generate-keystore.sh
RUN apt-get update && apt-get install -y --no-install-recommends bash \
 && bash certs/generate-keystore.sh certs/keystore.p12 \
 && rm -rf /var/lib/apt/lists/*
ENV MICRONAUT_ENVIRONMENTS=docker
# Arrow needs nio access on modern JDKs:
ENV JAVA_TOOL_OPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED --enable-native-access=ALL-UNNAMED"
EXPOSE 443
CMD ["java", "-jar", "app.jar"]
```

- [ ] **Step 4: Write .dockerignore + .gitattributes (LF for shell scripts)**

`java-webservice-template/.dockerignore`:
```
target/
certs/*.p12
.git/
```
`java-webservice-template/.gitattributes`:
```
*.sh text eol=lf
*.sql text eol=lf
```
Append `certs/*.p12` and `certs/keystore.p12` to `java-webservice-template/.gitignore`.

- [ ] **Step 5: Build the image + commit**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
docker build -t java-template-app .
cd c:/Users/Alexander/templates
git add java-webservice-template/certs java-webservice-template/src/main/resources/application-docker.yml java-webservice-template/Dockerfile java-webservice-template/.dockerignore java-webservice-template/.gitattributes java-webservice-template/.gitignore
git commit -m "build(java): self-signed HTTPS, docker image on 443"
```

---

## Task 3: Standalone Flight server + docker-compose stack

**Files:**
- Create: `.../flightserver/ExampleFlightServer.java`
- Create: `docker-compose.yml`, `docker-compose.profiling.yml`, `scripts/clickhouse-seed.sh`

- [ ] **Step 1: Write the standalone Flight server** (the `tests/publishers/flight_server.py` analogue; runnable via `java -cp app.jar ...ExampleFlightServer`)

`.../flightserver/ExampleFlightServer.java`:
```java
package com.example.template.flightserver;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.*;

import java.nio.charset.StandardCharsets;
import java.util.List;

/** Minimal Flight server that streams synthetic batches on getStream, for the
 *  docker-compose 'flight' service. Mirrors the Python example Flight server. */
public final class ExampleFlightServer {

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getenv().getOrDefault("FLIGHT_PORT", "8815"));
        long intervalMs = Long.parseLong(System.getenv().getOrDefault("FLIGHT_INTERVAL_MS", "1000"));
        try (RootAllocator allocator = new RootAllocator();
             FlightServer server = FlightServer.builder(allocator,
                 Location.forGrpcInsecure("0.0.0.0", port), new Producer(allocator, intervalMs)).build()) {
            server.start();
            System.out.println("Flight server on " + port);
            server.awaitTermination();
        }
    }

    static final class Producer extends NoOpFlightProducer {
        private final RootAllocator allocator;
        private final long intervalMs;
        private long next = 0;
        Producer(RootAllocator a, long intervalMs) { this.allocator = a; this.intervalMs = intervalMs; }

        @Override
        public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
            Schema schema = new Schema(List.of(
                new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.notNullable(new ArrowType.Utf8()), null),
                new Field("value", FieldType.notNullable(new ArrowType.Utf8()), null),
                new Field("op", FieldType.notNullable(new ArrowType.Utf8()), null)));
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                listener.start(root);
                for (int i = 0; i < 100; i++) {
                    long id = next++;
                    ((BigIntVector) root.getVector("id")).setSafe(0, id);
                    ((VarCharVector) root.getVector("name")).setSafe(0, ("n" + id).getBytes(StandardCharsets.UTF_8));
                    ((VarCharVector) root.getVector("value")).setSafe(0, ("v" + id).getBytes(StandardCharsets.UTF_8));
                    ((VarCharVector) root.getVector("op")).setSafe(0, "insert".getBytes(StandardCharsets.UTF_8));
                    root.setRowCount(1);
                    listener.putNext();
                    try { Thread.sleep(intervalMs); } catch (InterruptedException e) { break; }
                    root.clear();
                }
                listener.completed();
            }
        }
    }
}
```

- [ ] **Step 2: Write the ClickHouse seed script** (mirrors the Python `clickhouse-seed.sh`)

`java-webservice-template/scripts/clickhouse-seed.sh`:
```bash
#!/bin/bash
set -e
clickhouse-client --query="INSERT INTO default.items VALUES (1,'seed-a','x'),(2,'seed-b','y'),(3,'seed-c','z')"
```

- [ ] **Step 3: Write docker-compose.yml**

`java-webservice-template/docker-compose.yml` (compose project name `java-template` → network `java-template_default`):
```yaml
name: java-template
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: appuser
      POSTGRES_PASSWORD: password
      POSTGRES_DB: appdb
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U appuser -d appdb"]
      interval: 5s
      timeout: 3s
      retries: 10
  clickhouse:
    image: clickhouse/clickhouse-server:24.3
    volumes:
      - ./src/main/resources/db/clickhouse-init.sql:/docker-entrypoint-initdb.d/01-schema.sql
      - ./scripts/clickhouse-seed.sh:/docker-entrypoint-initdb.d/02-seed.sh
    ulimits:
      nofile: { soft: 262144, hard: 262144 }
  redis:
    image: redis/redis-stack-server:latest
  flight:
    image: java-template-app
    command: ["java", "-cp", "app.jar", "com.example.template.flightserver.ExampleFlightServer"]
    environment:
      FLIGHT_PORT: "8815"
  app:
    image: java-template-app
    depends_on:
      postgres: { condition: service_healthy }
      clickhouse: { condition: service_started }
      redis: { condition: service_started }
      flight: { condition: service_started }
    ports:
      - "443:443"
    environment:
      MICRONAUT_ENVIRONMENTS: docker
      INGEST_TRANSPORT: flight
    healthcheck:
      test: ["CMD-SHELL", "curl -fsk https://localhost/health/live || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 10
```

- [ ] **Step 4: Write the profiling overlay** (async-profiler needs perf-event access)

`java-webservice-template/docker-compose.profiling.yml`:
```yaml
name: java-template
services:
  app:
    cap_add:
      - SYS_ADMIN     # async-profiler perf events
    security_opt:
      - seccomp:unconfined
  flight:
    environment:
      FLIGHT_INTERVAL_MS: "3600000"  # idle stream so HTTP is the sole writer
```

- [ ] **Step 5: Bring up + smoke test + commit**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
docker compose up -d --build --wait
curl -fsk https://localhost/health/ready | head -c 200
docker compose down
cd c:/Users/Alexander/templates
git add java-webservice-template/src/main/java/com/example/template/flightserver java-webservice-template/scripts java-webservice-template/docker-compose.yml java-webservice-template/docker-compose.profiling.yml
git commit -m "feat(java): standalone flight server + docker-compose stack"
```

---

## Task 4: k6 performance scripts (reuse) + async-profiler runbook

**Files:**
- Create: `tests/performance/**` (copied from Python template), `tests/performance/profile/run_asyncprofiler.sh`, `PERF_PROFILING_RUNBOOK.md`

- [ ] **Step 1: Copy the k6 scripts verbatim and retarget**

```bash
cd c:/Users/Alexander/templates
cp -r python-webservice-template/tests/performance java-webservice-template/tests/performance
# Remove Python-specific profiling bits; keep the .js scripts + lib/ + data/
rm -rf java-webservice-template/tests/performance/profile
rm -f java-webservice-template/tests/performance/publishers/solace_publisher.py 2>/dev/null || true
ls java-webservice-template/tests/performance
```

The k6 `.js` scripts already default `BASE_URL` to `https://localhost`; in compose/CI use `BASE_URL=https://app`. The `Server-Timing` attribution scripts (`profile_reads.js`, `profile_ingest.js`) work unchanged because Phase 2/3/4 emit the same header token format.

- [ ] **Step 2: Write the async-profiler runbook script** (Layer-2; py-spy analogue)

`java-webservice-template/tests/performance/profile/run_asyncprofiler.sh`:
```bash
#!/usr/bin/env bash
# Attach async-profiler to the running app container and capture a wall-clock and
# a CPU flamegraph (the py-spy --idle/--gil analogue). Requires the profiling
# overlay (docker-compose.profiling.yml) for SYS_ADMIN. Run a k6 profile under
# load in another shell so samples land while the app is busy.
#
# Usage: tests/performance/profile/run_asyncprofiler.sh [DURATION_SECONDS]
set -euo pipefail
DURATION="${1:-30}"
OUT_DIR="$(cd "$(dirname "$0")" && pwd)/artifacts"
mkdir -p "$OUT_DIR"
cd "$(dirname "$0")/../../.."

APP_CID="$(docker compose ps -q app)"
if [ -z "$APP_CID" ]; then
  echo "app container not running. Start the profiling stack first:" >&2
  echo "  docker compose -f docker-compose.yml -f docker-compose.profiling.yml up -d --build" >&2
  exit 1
fi

# Install async-profiler into the container (ephemeral).
MSYS_NO_PATHCONV=1 docker exec "$APP_CID" sh -c '
  test -d /opt/async-profiler || (
    apt-get update -qq && apt-get install -y -qq curl &&
    curl -sL https://github.com/async-profiler/async-profiler/releases/latest/download/async-profiler-linux-x64.tar.gz | tar xz -C /opt &&
    mv /opt/async-profiler-* /opt/async-profiler )'

APP_PID="$(MSYS_NO_PATHCONV=1 docker exec "$APP_CID" sh -c "pgrep -f app.jar | head -1")"
echo "Profiling app pid=$APP_PID for ${DURATION}s..."

# (1) Wall-clock flamegraph (overall time, incl. parked threads).
MSYS_NO_PATHCONV=1 docker exec "$APP_CID" /opt/async-profiler/bin/asprof \
  -e wall -d "$DURATION" -f /tmp/flame_wall.html "$APP_PID"
docker cp "$APP_CID:/tmp/flame_wall.html" "$OUT_DIR/flame_wall.html"

# (2) CPU flamegraph (on-CPU only).
MSYS_NO_PATHCONV=1 docker exec "$APP_CID" /opt/async-profiler/bin/asprof \
  -e cpu -d "$DURATION" -f /tmp/flame_cpu.html "$APP_PID"
docker cp "$APP_CID:/tmp/flame_cpu.html" "$OUT_DIR/flame_cpu.html"

echo "Artifacts written to $OUT_DIR: flame_wall.html, flame_cpu.html"
```

- [ ] **Step 3: Write PERF_PROFILING_RUNBOOK.md** (adapt the Python runbook to async-profiler)

`java-webservice-template/PERF_PROFILING_RUNBOOK.md` — Layer 1 (k6 + Server-Timing attribution, identical to Python), Layer 2 (async-profiler: `flame_wall.html` = wall time, `flame_cpu.html` = on-CPU; sparse CPU ⇒ I/O-bound, same decision rule). Two-shell pattern: k6 drives `BASE_URL=https://app` load while `run_asyncprofiler.sh` samples.

- [ ] **Step 4: Add artifacts to .gitignore + commit**

```bash
cd c:/Users/Alexander/templates
printf "\ntests/performance/profile/artifacts/\n" >> java-webservice-template/.gitignore
git add java-webservice-template/tests/performance java-webservice-template/PERF_PROFILING_RUNBOOK.md java-webservice-template/.gitignore
git commit -m "perf(java): reuse k6 scripts + async-profiler runbook"
```

---

## Task 5: JaCoCo coverage gate

**Files:** Modify `pom.xml`

- [ ] **Step 1: Add the JaCoCo plugin with a coverage rule**

In `pom.xml` `<build><plugins>`:
```xml
<plugin>
  <groupId>org.jacoco</groupId>
  <artifactId>jacoco-maven-plugin</artifactId>
  <version>0.8.12</version>
  <executions>
    <execution><id>prepare</id><goals><goal>prepare-agent</goal></goals></execution>
    <execution>
      <id>check</id>
      <phase>verify</phase>
      <goals><goal>check</goal></goals>
      <configuration>
        <rules>
          <rule>
            <element>BUNDLE</element>
            <limits>
              <limit><counter>LINE</counter><value>COVEREDRATIO</value><minimum>0.80</minimum></limit>
            </limits>
            <excludes>
              <exclude>com.example.template.Application</exclude>
              <exclude>com.example.template.flightserver.*</exclude>
            </excludes>
          </rule>
        </rules>
      </configuration>
    </execution>
  </executions>
</plugin>
```

> Note: the Python gate is 94% (`.coveragerc`). The JVM template starts at **80%** because integration-only coverage of transport edge cases (Flight/Solace reconnect) is lower without unit-level fakes; raise toward parity as unit tests are added. Document the current number in README.

- [ ] **Step 2: Verify the gate runs**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q verify
```
Expected: `BUILD SUCCESS` with a JaCoCo coverage check; if below threshold, the build fails (tune the minimum or add tests).

- [ ] **Step 3: Commit**

```bash
cd c:/Users/Alexander/templates
git add java-webservice-template/pom.xml
git commit -m "build(java): JaCoCo coverage gate"
```

---

## Task 6: GitLab CI

**Files:** Create `java-webservice-template/.gitlab-ci.yml`

- [ ] **Step 1: Write the pipeline** (mirrors the Python pipeline: build/test gate + k6 jobs)

`java-webservice-template/.gitlab-ci.yml`:
```yaml
stages: [test, perf]

variables:
  MAVEN_OPTS: "-Dmaven.repo.local=.m2/repository"

cache:
  paths: [.m2/repository]

build-test:
  stage: test
  image: maven:3.9-eclipse-temurin-25
  services:
    - docker:dind
  variables:
    DOCKER_HOST: tcp://docker:2375
    TESTCONTAINERS_HOST_OVERRIDE: docker
  script:
    - cd java-webservice-template
    - ./mvnw -B verify
  artifacts:
    when: always
    reports:
      junit: java-webservice-template/target/surefire-reports/TEST-*.xml

.k6:
  stage: perf
  image: docker:latest
  services: [docker:dind]
  script:
    - cd java-webservice-template
    - docker compose up -d --build --wait
    - docker build -t perf-scripts ./tests/performance
    - docker run --rm --network java-template_default -e BASE_URL=https://app perf-scripts run "/scripts/$SCRIPT"
  after_script:
    - cd java-webservice-template && docker compose down -v

k6-smoke:
  extends: .k6
  variables: { SCRIPT: smoke.js }

k6-load:
  extends: .k6
  variables: { SCRIPT: load.js }

k6-stress:
  extends: .k6
  allow_failure: true
  variables: { SCRIPT: stress.js }
```

- [ ] **Step 2: Commit**

```bash
cd c:/Users/Alexander/templates
git add java-webservice-template/.gitlab-ci.yml
git commit -m "ci(java): gitlab pipeline — maven verify + k6 gates"
```

---

## Task 7: README + CLAUDE.md

**Files:** Create/expand `java-webservice-template/README.md`, `java-webservice-template/CLAUDE.md`

- [ ] **Step 1: Write CLAUDE.md** — the architecture map, mirroring the Python `CLAUDE.md` section-for-section, with a Python↔Java mapping table at the top and notes on the deliberate divergences (native DI, append-only LSM, `/data/cache` seqno+op, Jedis, async-profiler, MCP bridge approach chosen in Task 1).

- [ ] **Step 2: Expand README.md** — navigational map with clickable links to each feature's Java entry point (controllers, services, filters, stores, ingestion), the run/test/profile instructions, and an explicit "How this maps to python-webservice-template" section. Mirror the structure of the Python `README.md`. State the current JaCoCo coverage number.

- [ ] **Step 3: Commit**

```bash
cd c:/Users/Alexander/templates
git add java-webservice-template/README.md java-webservice-template/CLAUDE.md
git commit -m "docs(java): README + CLAUDE.md with Python<->Java mapping"
```

---

## Task 8: Final verification + root docs update

- [ ] **Step 1: Full local gate**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q verify        # JUnit + JaCoCo (Docker running)
docker compose -f docker-compose.yml -f docker-compose.profiling.yml up -d --build --wait
docker build -t perf-scripts ./tests/performance
docker run --rm --network java-template_default -e BASE_URL=https://app -e DURATION=10s perf-scripts run /scripts/smoke.js
docker compose down -v
```
Expected: tests green, k6 smoke passes against the Java service over HTTPS.

- [ ] **Step 2: Update the repo root CLAUDE.md progress note**

In `c:/Users/Alexander/templates/CLAUDE.md`, change the "Current Progress" note to record that `java-webservice-template` is now ready alongside the Python one.

- [ ] **Step 3: Commit**

```bash
cd c:/Users/Alexander/templates
git add CLAUDE.md
git commit -m "docs: mark java-webservice-template ready"
```

---

## Phase 5 self-review checklist
- [ ] `/mcp` is mounted and exposes `get_health_status` (spike resolved, `McpEndpointTest` green).
- [ ] `docker compose up --wait` brings the full stack healthy; app serves HTTPS on 443.
- [ ] k6 smoke/load pass against the Java service; `profile_reads.js` prints a Server-Timing attribution table.
- [ ] `run_asyncprofiler.sh` produces `flame_wall.html` + `flame_cpu.html` under load.
- [ ] `./mvnw verify` enforces the JaCoCo gate; CI runs verify + k6.
- [ ] README/CLAUDE.md let a reader map every Java feature to its Python counterpart.

## Template complete
With Phases 1–5 done, `java-webservice-template` mirrors `python-webservice-template` feature-for-feature with the five documented divergences (native DI, append-only LSM, `/data/cache` seqno+op, Jedis, async-profiler) plus the MCP-bridge approach chosen during the Phase-5 spike.
