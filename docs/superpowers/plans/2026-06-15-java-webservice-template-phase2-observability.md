# Java Webservice Template — Phase 2: Health + Observability Core

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the cross-cutting observability and resilience surface — typed app settings, correlation-ID propagation, Server-Timing boundary attribution, the three health endpoints, system metrics, Prometheus `/metrics`, CORS, and the inbound body-size limit — mirroring `python-webservice-template/{core,services/health,services/metrics,schemas/health}`.

**Architecture:** Cross-cutting concerns are Micronaut `@ServerFilter`s and `@Singleton`s. Correlation ID rides Micronaut 4's `PropagatedContext` so it survives the event-loop→virtual-thread hop and lands in MDC for every log line. Server-Timing samples live on an `HttpRequest` attribute (reachable via `ServerRequestContext`) so the handler appends and the response filter renders. Health uses an idiomatic **probe registry**: `HealthService` injects `List<DependencyHealthProbe>`, so each store added in later phases self-registers — `ConfigService` provides the only probe this phase. Ingest health comes from an injected `IngestHealthProvider` with a default "not-configured" bean until Phase 4 supplies the real one.

**Tech Stack:** Micronaut 4.8.x, Java 25, SLF4J/Logback MDC, Micronaut Micrometer (Prometheus registry), `com.sun.management.OperatingSystemMXBean`.

**Reference:** `core/correlation.py`, `core/boundary_timing.py`, `core/request_limits.py`, `core/system_metrics.py`, `core/container.py`, `services/health.py`, `services/metrics.py`, `schemas/health.py`, `settings.py`.

**Conventions:** as Phase 1 — module at `java-webservice-template/`, run Maven from there, package root `com.example.template`.

---

## File structure produced by this phase

| File | Responsibility |
|---|---|
| `pom.xml` | + Micrometer Prometheus + management dependencies |
| `.../config/AppSettings.java` | Typed app config (`settings.py` analogue, non-datasource fields) |
| `src/main/resources/application.yml` | + `template.*` config, CORS, max-request-size, metrics |
| `src/main/resources/logback.xml` | + `%X{correlationId}` MDC pattern |
| `.../core/CorrelationFilter.java` | Adopt/generate `X-Request-ID`, push to MDC via PropagatedContext, echo header |
| `.../core/BoundarySamples.java` | Request-scoped boundary sample list + render |
| `.../core/Timed.java` | `try-with-resources` boundary timer (`timed()` analogue) |
| `.../core/ServerTimingFilter.java` | Install sample list, render `Server-Timing` on response |
| `.../core/LastRequestTracker.java` | Tracks `lastRequestAt` (`container.last_request_at` analogue) |
| `.../core/SystemMetrics.java` | Process/host snapshot (`system_metrics.py` analogue) |
| `.../health/DependencyHealthProbe.java` | Interface every store implements for readiness |
| `.../health/IngestHealthProvider.java` + `DefaultIngestHealthProvider.java` | Ingest health seam (real impl in Phase 4) |
| `.../dto/health/*.java` | Health response records (mirror `schemas/health.py`) |
| `.../service/HealthService.java` | Liveness/readiness/detailed status |
| `.../service/ConfigHealthProbe.java` | Postgres probe (wraps `ConfigurationRepository`) |
| `.../controller/HealthController.java` | `/health/{live,ready,status}` |
| `.../observability/MetricsBinder.java` + `MetricsController.java` | Custom gauges + `/metrics` scrape |
| Tests for correlation, server-timing, health, body-size, metrics |

---

## Task 1: Add observability dependencies

**Files:** Modify `java-webservice-template/pom.xml`

- [ ] **Step 1: Add Micrometer + management dependencies**

In `<dependencies>` add:

```xml
<dependency><groupId>io.micronaut.micrometer</groupId><artifactId>micronaut-micrometer-registry-prometheus</artifactId><scope>compile</scope></dependency>
<dependency><groupId>io.micronaut</groupId><artifactId>micronaut-management</artifactId><scope>compile</scope></dependency>
```

- [ ] **Step 2: Verify resolve/compile**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q -DskipTests compile
```
Expected: `BUILD SUCCESS`.

- [ ] **Step 3: Commit**

```bash
cd c:/Users/Alexander/templates
git add java-webservice-template/pom.xml
git commit -m "build(java): add micrometer prometheus + management deps"
```

---

## Task 2: Typed application settings

**Files:**
- Create: `.../config/AppSettings.java`
- Modify: `src/main/resources/application.yml`

- [ ] **Step 1: Add config to application.yml**

Append under root, and add CORS + max body + metrics blocks:

```yaml
template:
  app-title: "Template Micronaut Service"
  app-version: "1.0.0"
  app-description: "A Java Micronaut service mirroring the Python webservice template"
  status: "running"
  correlation-id-header: "X-Request-ID"
  health-check-timeout-seconds: 2.0
  max-request-body-bytes: 16777216
  mcp-name: "java-template"
  mcp-instructions: "Tools for this template application."

micronaut:
  server:
    max-request-size: 16MB
    cors:
      enabled: true
      configurations:
        default:
          allowed-origins:
            - "*"
  metrics:
    enabled: true
    binders:
      jvm:
        enabled: true
      processor:
        enabled: true
      uptime:
        enabled: true
    export:
      prometheus:
        enabled: true
        step: PT1M

endpoints:
  prometheus:
    enabled: true
    sensitive: false
```

- [ ] **Step 2: Write AppSettings**

`.../config/AppSettings.java` (the `settings.py` analogue for non-datasource fields; env vars override via Micronaut property resolution):

```java
package com.example.template.config;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("template")
public class AppSettings {
    private String appTitle = "Template Micronaut Service";
    private String appVersion = "1.0.0";
    private String appDescription = "";
    private String status = "running";
    private String correlationIdHeader = "X-Request-ID";
    private double healthCheckTimeoutSeconds = 2.0;
    private long maxRequestBodyBytes = 16L * 1024 * 1024;
    private String mcpName = "java-template";
    private String mcpInstructions = "Tools for this template application.";

    public String getAppTitle() { return appTitle; }
    public void setAppTitle(String v) { this.appTitle = v; }
    public String getAppVersion() { return appVersion; }
    public void setAppVersion(String v) { this.appVersion = v; }
    public String getAppDescription() { return appDescription; }
    public void setAppDescription(String v) { this.appDescription = v; }
    public String getStatus() { return status; }
    public void setStatus(String v) { this.status = v; }
    public String getCorrelationIdHeader() { return correlationIdHeader; }
    public void setCorrelationIdHeader(String v) { this.correlationIdHeader = v; }
    public double getHealthCheckTimeoutSeconds() { return healthCheckTimeoutSeconds; }
    public void setHealthCheckTimeoutSeconds(double v) { this.healthCheckTimeoutSeconds = v; }
    public long getMaxRequestBodyBytes() { return maxRequestBodyBytes; }
    public void setMaxRequestBodyBytes(long v) { this.maxRequestBodyBytes = v; }
    public String getMcpName() { return mcpName; }
    public void setMcpName(String v) { this.mcpName = v; }
    public String getMcpInstructions() { return mcpInstructions; }
    public void setMcpInstructions(String v) { this.mcpInstructions = v; }
}
```

- [ ] **Step 3: Compile + commit**

```bash
cd c:/Users/Alexander/templates/java-webservice-template && ./mvnw -q -DskipTests compile
cd c:/Users/Alexander/templates
git add java-webservice-template/src/main/java/com/example/template/config/AppSettings.java java-webservice-template/src/main/resources/application.yml
git commit -m "feat(java): typed AppSettings + cors/metrics/body-size config"
```

---

## Task 3: Correlation-ID propagation (filter + MDC) — TDD

**Files:**
- Create: `.../core/CorrelationFilter.java`
- Modify: `src/main/resources/logback.xml`
- Test: `.../core/CorrelationFilterTest.java`

> **Integration note:** correlation must survive the event-loop→virtual-thread hop and appear in MDC for handler logs. Micronaut 4's `PropagatedContext` with an MDC element does this. The test below is the contract; if the exact MDC-propagation API differs in the pinned Micronaut version, adjust the filter until the test passes (the test, not the incantation, defines done).

- [ ] **Step 1: Write the failing test**

`.../core/CorrelationFilterTest.java`:

```java
package com.example.template.core;

import io.micronaut.http.HttpHeaders;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import static org.assertj.core.api.Assertions.assertThat;

@MicronautTest
class CorrelationFilterTest {

    @Controller("/__corrtest")
    @ExecuteOn(TaskExecutors.BLOCKING)
    static class Probe {
        @Get
        String mdcValue() {
            // Runs on the (virtual) handler thread: the id must have propagated here.
            return String.valueOf(MDC.get("correlationId"));
        }
    }

    @Inject
    @Client("/")
    HttpClient client;

    @Test
    void generatesIdEchoesHeaderAndPopulatesMdc() {
        HttpResponse<String> resp = client.toBlocking().exchange(
            HttpRequest.GET("/__corrtest"), String.class);
        String header = resp.getHeaders().get("X-Request-ID");
        assertThat(header).isNotBlank();
        assertThat(resp.body()).isEqualTo(header); // MDC on handler thread == echoed id
    }

    @Test
    void adoptsInboundId() {
        HttpResponse<String> resp = client.toBlocking().exchange(
            HttpRequest.GET("/__corrtest").header("X-Request-ID", "abc123"), String.class);
        assertThat(resp.getHeaders().get("X-Request-ID")).isEqualTo("abc123");
        assertThat(resp.body()).isEqualTo("abc123");
    }
}
```

- [ ] **Step 2: Run → fail**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test -Dtest=CorrelationFilterTest
```
Expected: FAIL — no header echoed / MDC is `null`.

- [ ] **Step 3: Implement the filter**

`.../core/CorrelationFilter.java`:

```java
package com.example.template.core;

import com.example.template.config.AppSettings;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.order.Ordered;
import io.micronaut.core.propagation.PropagatedContext;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.filter.HttpServerFilter;
import io.micronaut.http.filter.ServerFilterChain;
import io.micronaut.logging.LogLevel;
import org.reactivestreams.Publisher;
import org.slf4j.MDC;

import java.util.UUID;

@Filter("/**")
public class CorrelationFilter implements HttpServerFilter {

    private final String header;

    public CorrelationFilter(AppSettings settings) {
        this.header = settings.getCorrelationIdHeader();
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 20; // outer-ish, but inside CORS
    }

    @Override
    public Publisher<MutableHttpResponse<?>> doFilter(HttpRequest<?> request, ServerFilterChain chain) {
        String incoming = request.getHeaders().get(header);
        String cid = (incoming != null && !incoming.isBlank()) ? incoming : UUID.randomUUID().toString();
        request.setAttribute("correlationId", cid);

        // MdcPropagationContext carries the MDC entry across the thread hop to the
        // @ExecuteOn(BLOCKING) handler; PropagatedContext.propagate installs it.
        try (MDC.MDCCloseable ignored = MDC.putCloseable("correlationId", cid);
             PropagatedContext.Scope scope =
                 PropagatedContext.getOrEmpty().plus(new MdcPropagationContext(cid)).propagate()) {
            return Publishers.map(chain.proceed(request), resp -> {
                resp.getHeaders().add(header, cid);
                return resp;
            });
        }
    }
}
```

`.../core/MdcPropagationContext.java` (propagates the MDC value onto whichever thread continues the request):

```java
package com.example.template.core;

import io.micronaut.core.propagation.ThreadPropagatedContextElement;
import org.slf4j.MDC;

public record MdcPropagationContext(String correlationId) implements ThreadPropagatedContextElement<String> {
    @Override
    public String updateThreadContext() {
        String previous = MDC.get("correlationId");
        MDC.put("correlationId", correlationId);
        return previous;
    }

    @Override
    public void restoreThreadContext(String previous) {
        if (previous == null) {
            MDC.remove("correlationId");
        } else {
            MDC.put("correlationId", previous);
        }
    }
}
```

- [ ] **Step 4: Add MDC to logback pattern**

In `src/main/resources/logback.xml`, change the pattern line to include the correlation id:

```xml
<pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} %-5level [%X{correlationId:-}] [%thread] %logger{36} - %msg%n</pattern>
```

- [ ] **Step 5: Run → pass**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test -Dtest=CorrelationFilterTest
```
Expected: PASS (2 tests). If `ThreadPropagatedContextElement` is not resolvable in the pinned version, use `io.micronaut.core.propagation.PropagatedContextElement` with the MDC update done in the filter's `try` block around a `chain.proceed` collected on the handler — adjust until green.

- [ ] **Step 6: Commit**

```bash
cd c:/Users/Alexander/templates
git add java-webservice-template/src/main/java/com/example/template/core java-webservice-template/src/main/resources/logback.xml java-webservice-template/src/test/java/com/example/template/core/CorrelationFilterTest.java
git commit -m "feat(java): correlation-id filter with MDC propagation"
```

---

## Task 4: Server-Timing boundary attribution — TDD

**Files:**
- Create: `.../core/BoundarySamples.java`, `.../core/Timed.java`, `.../core/ServerTimingFilter.java`
- Test: `.../core/ServerTimingFilterTest.java`

- [ ] **Step 1: Write the failing test**

`.../core/ServerTimingFilterTest.java`:

```java
package com.example.template.core;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@MicronautTest
class ServerTimingFilterTest {

    @Controller("/__sttest")
    @ExecuteOn(TaskExecutors.BLOCKING)
    static class Probe {
        @Get
        String work() throws InterruptedException {
            try (Timed t = Timed.start("db.query")) {
                Thread.sleep(5);
            }
            return "ok";
        }
    }

    @Inject
    @Client("/")
    HttpClient client;

    @Test
    void emitsServerTimingHeaderWithBoundaryAndTotal() {
        HttpResponse<String> resp = client.toBlocking().exchange(HttpRequest.GET("/__sttest"), String.class);
        String st = resp.getHeaders().get("Server-Timing");
        assertThat(st).contains("db_query;dur=").contains("total;dur=");
    }
}
```

- [ ] **Step 2: Run → fail**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test -Dtest=ServerTimingFilterTest
```
Expected: FAIL — compile error (`Timed` missing) / no header.

- [ ] **Step 3: Implement BoundarySamples, Timed, filter**

`.../core/BoundarySamples.java`:

```java
package com.example.template.core;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.context.ServerRequestContext;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/** Request-scoped per-boundary timings, rendered as a W3C Server-Timing header.
 *  Stored on the HttpRequest attribute so the handler thread (via
 *  ServerRequestContext) appends and the response filter renders. */
public final class BoundarySamples {

    public static final String ATTR = "boundarySamples";
    private static final Pattern NON_TOKEN = Pattern.compile("[^A-Za-z0-9_]");

    private final List<Map.Entry<String, Double>> samples = new ArrayList<>();

    public synchronized void add(String label, double ms) {
        samples.add(Map.entry(label, ms));
    }

    public static void record(String label, double ms) {
        ServerRequestContext.currentRequest().ifPresent(req -> {
            Object holder = req.getAttribute(ATTR).orElse(null);
            if (holder instanceof BoundarySamples bs) {
                bs.add(label, ms);
            }
        });
    }

    public synchronized String render(double totalMs) {
        Map<String, Double> agg = new LinkedHashMap<>();
        for (var e : samples) {
            String token = NON_TOKEN.matcher(e.getKey()).replaceAll("_");
            agg.merge(token, e.getValue(), Double::sum);
        }
        StringBuilder sb = new StringBuilder();
        for (var e : agg.entrySet()) {
            sb.append(e.getKey()).append(";dur=").append(String.format("%.2f", e.getValue())).append(", ");
        }
        sb.append("total;dur=").append(String.format("%.2f", totalMs));
        return sb.toString();
    }
}
```

`.../core/Timed.java`:

```java
package com.example.template.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** try-with-resources boundary timer; mirrors Python core.correlation.timed().
 *  Logs the duration and records a Server-Timing sample for the current request. */
public final class Timed implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(Timed.class);
    private final String label;
    private final long start;

    private Timed(String label) {
        this.label = label;
        this.start = System.nanoTime();
    }

    public static Timed start(String label) {
        return new Timed(label);
    }

    @Override
    public void close() {
        double ms = (System.nanoTime() - start) / 1_000_000.0;
        LOG.debug("{} {}ms", label, String.format("%.2f", ms));
        BoundarySamples.record(label, ms);
    }
}
```

`.../core/ServerTimingFilter.java`:

```java
package com.example.template.core;

import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.core.order.Ordered;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.filter.HttpServerFilter;
import io.micronaut.http.filter.ServerFilterChain;
import org.reactivestreams.Publisher;

@Filter("/**")
public class ServerTimingFilter implements HttpServerFilter {

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE; // innermost: wraps the handler, sees all boundaries
    }

    @Override
    public Publisher<MutableHttpResponse<?>> doFilter(HttpRequest<?> request, ServerFilterChain chain) {
        BoundarySamples samples = new BoundarySamples();
        request.setAttribute(BoundarySamples.ATTR, samples);
        long start = System.nanoTime();
        return Publishers.map(chain.proceed(request), resp -> {
            double totalMs = (System.nanoTime() - start) / 1_000_000.0;
            resp.getHeaders().add("Server-Timing", samples.render(totalMs));
            return resp;
        });
    }
}
```

- [ ] **Step 4: Run → pass**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test -Dtest=ServerTimingFilterTest
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd c:/Users/Alexander/templates
git add java-webservice-template/src/main/java/com/example/template/core/BoundarySamples.java java-webservice-template/src/main/java/com/example/template/core/Timed.java java-webservice-template/src/main/java/com/example/template/core/ServerTimingFilter.java java-webservice-template/src/test/java/com/example/template/core/ServerTimingFilterTest.java
git commit -m "feat(java): Server-Timing boundary attribution"
```

---

## Task 5: Last-request tracker + system metrics

**Files:**
- Create: `.../core/LastRequestTracker.java`, `.../core/LastRequestFilter.java`, `.../core/SystemMetrics.java`
- Test: `.../core/SystemMetricsTest.java`

- [ ] **Step 1: Write LastRequestTracker + filter**

`.../core/LastRequestTracker.java`:

```java
package com.example.template.core;

import jakarta.inject.Singleton;
import java.time.Instant;

/** Holds the timestamp of the most recent request — the container.last_request_at analogue. */
@Singleton
public class LastRequestTracker {
    private volatile Instant lastRequestAt;

    public void mark() { this.lastRequestAt = Instant.now(); }
    public Instant lastRequestAt() { return lastRequestAt; }
}
```

`.../core/LastRequestFilter.java`:

```java
package com.example.template.core;

import io.micronaut.core.order.Ordered;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.filter.HttpServerFilter;
import io.micronaut.http.filter.ServerFilterChain;
import org.reactivestreams.Publisher;

@Filter("/**")
public class LastRequestFilter implements HttpServerFilter {
    private final LastRequestTracker tracker;

    public LastRequestFilter(LastRequestTracker tracker) { this.tracker = tracker; }

    @Override
    public int getOrder() { return Ordered.LOWEST_PRECEDENCE - 10; }

    @Override
    public Publisher<MutableHttpResponse<?>> doFilter(HttpRequest<?> request, ServerFilterChain chain) {
        tracker.mark();
        return chain.proceed(request);
    }
}
```

- [ ] **Step 2: Write the failing SystemMetrics test**

`.../core/SystemMetricsTest.java`:

```java
package com.example.template.core;

import com.example.template.dto.health.SystemSnapshot;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SystemMetricsTest {
    @Test
    void snapshotHasPositiveProcessAndHostMemory() {
        SystemSnapshot snap = new SystemMetrics().snapshot();
        assertThat(snap.process().memoryRssBytes()).isGreaterThan(0);
        assertThat(snap.process().numThreads()).isGreaterThan(0);
        assertThat(snap.host().memoryTotalBytes()).isGreaterThan(0);
    }
}
```

- [ ] **Step 3: Run → fail** (DTOs + SystemMetrics missing)

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test -Dtest=SystemMetricsTest
```
Expected: FAIL (compile — `SystemSnapshot`, `SystemMetrics` missing). These DTOs are created in Task 6; implement `SystemMetrics` after the DTOs and re-run. (If executing strictly in order, do Task 6 DTOs first, then return here.)

- [ ] **Step 4: Write SystemMetrics**

`.../core/SystemMetrics.java` (the `system_metrics.py`/psutil analogue via JMX):

```java
package com.example.template.core;

import com.example.template.dto.health.HostStats;
import com.example.template.dto.health.ProcessStats;
import com.example.template.dto.health.SystemSnapshot;
import jakarta.inject.Singleton;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

@Singleton
public class SystemMetrics {

    private final com.sun.management.OperatingSystemMXBean os =
        (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    private final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
    private final Runtime runtime = Runtime.getRuntime();

    public SystemSnapshot snapshot() {
        long totalMem = os.getTotalMemorySize();
        long freeMem = os.getFreeMemorySize();
        double hostCpu = os.getCpuLoad() < 0 ? 0.0 : os.getCpuLoad() * 100.0;
        double procCpu = os.getProcessCpuLoad() < 0 ? 0.0 : os.getProcessCpuLoad() * 100.0;
        long rss = runtime.totalMemory() - runtime.freeMemory();
        double usedPct = totalMem > 0 ? (totalMem - freeMem) * 100.0 / totalMem : 0.0;

        ProcessStats process = new ProcessStats(procCpu, rss, threads.getThreadCount(), 0);
        HostStats host = new HostStats(hostCpu, totalMem, freeMem, usedPct);
        return new SystemSnapshot(process, host);
    }
}
```

(Note: `open_files` has no portable JMX equivalent; reported as 0, a documented divergence from psutil.)

- [ ] **Step 5: Run → pass** (after Task 6 DTOs exist)

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test -Dtest=SystemMetricsTest
```
Expected: PASS.

- [ ] **Step 6: Commit** (commit together with Task 6)

---

## Task 6: Health DTOs

**Files:** Create records under `.../dto/health/` mirroring `schemas/health.py`.

- [ ] **Step 1: Write the DTOs**

Create each file under `src/main/java/com/example/template/dto/health/`. All are `@Serdeable` records; `null` fields are omitted from JSON via per-field handling in the controller (Micronaut Serde omits nulls by default for records with `@Serdeable`).

`ProbeResult.java`:
```java
package com.example.template.dto.health;
import io.micronaut.serde.annotation.Serdeable;
@Serdeable
public record ProbeResult(String name, String status, double latencyMs, String error) {
    public static ProbeResult up(String name, double latencyMs) { return new ProbeResult(name, "up", latencyMs, null); }
    public static ProbeResult down(String name, double latencyMs, String error) { return new ProbeResult(name, "down", latencyMs, error); }
}
```

`IngestHealth.java`:
```java
package com.example.template.dto.health;
import io.micronaut.serde.annotation.Serdeable;
import java.time.Instant;
@Serdeable
public record IngestHealth(String transport, String connectionState, boolean threadAlive,
                           Instant lastBatchAt, Double secondsSinceLastBatch,
                           long rowsIngestedTotal, boolean stale) {}
```

`CheckResult.java`:
```java
package com.example.template.dto.health;
import io.micronaut.serde.annotation.Serdeable;
import java.time.Instant;
@Serdeable
public record CheckResult(String name, String status, Double latencyMs, String transport,
                          String connectionState, Boolean threadAlive, Instant lastBatchAt,
                          Double secondsSinceLastBatch, String error) {}
```

`LivenessResponse.java`:
```java
package com.example.template.dto.health;
import io.micronaut.serde.annotation.Serdeable;
@Serdeable
public record LivenessResponse(String status, double uptimeSeconds) {}
```

`ReadinessResponse.java`:
```java
package com.example.template.dto.health;
import io.micronaut.serde.annotation.Serdeable;
import java.util.List;
@Serdeable
public record ReadinessResponse(String status, List<CheckResult> checks) {}
```

`ProcessStats.java`:
```java
package com.example.template.dto.health;
import io.micronaut.serde.annotation.Serdeable;
@Serdeable
public record ProcessStats(double cpuPercent, long memoryRssBytes, int numThreads, int openFiles) {}
```

`HostStats.java`:
```java
package com.example.template.dto.health;
import io.micronaut.serde.annotation.Serdeable;
@Serdeable
public record HostStats(double cpuPercent, long memoryTotalBytes, long memoryAvailableBytes, double memoryPercent) {}
```

`SystemSnapshot.java`:
```java
package com.example.template.dto.health;
import io.micronaut.serde.annotation.Serdeable;
@Serdeable
public record SystemSnapshot(ProcessStats process, HostStats host) {}
```

`AppInfo.java`:
```java
package com.example.template.dto.health;
import io.micronaut.serde.annotation.Serdeable;
@Serdeable
public record AppInfo(String title, String version, String status) {}
```

`UptimeInfo.java`:
```java
package com.example.template.dto.health;
import io.micronaut.serde.annotation.Serdeable;
@Serdeable
public record UptimeInfo(double processSeconds, double systemBootSeconds) {}
```

`RequestInfo.java`:
```java
package com.example.template.dto.health;
import io.micronaut.serde.annotation.Serdeable;
import java.time.Instant;
@Serdeable
public record RequestInfo(Instant lastRequestAt) {}
```

`DetailedStatusResponse.java`:
```java
package com.example.template.dto.health;
import io.micronaut.serde.annotation.Serdeable;
import java.util.List;
@Serdeable
public record DetailedStatusResponse(AppInfo app, UptimeInfo uptime, List<ProbeResult> dependencies,
                                     IngestHealth ingest, RequestInfo requests, SystemSnapshot system) {}
```

- [ ] **Step 2: Compile**

```bash
cd c:/Users/Alexander/templates/java-webservice-template && ./mvnw -q -DskipTests compile
```
Expected: `BUILD SUCCESS`. Now Task 5's `SystemMetricsTest` can pass — run `./mvnw -q test -Dtest=SystemMetricsTest` (PASS).

- [ ] **Step 3: Commit (Tasks 5+6 together)**

```bash
cd c:/Users/Alexander/templates
git add java-webservice-template/src/main/java/com/example/template/core/LastRequest*.java java-webservice-template/src/main/java/com/example/template/core/SystemMetrics.java java-webservice-template/src/main/java/com/example/template/dto/health java-webservice-template/src/test/java/com/example/template/core/SystemMetricsTest.java
git commit -m "feat(java): health DTOs, system metrics, last-request tracker"
```

---

## Task 7: Probe registry + ingest seam + ConfigService probe

**Files:**
- Create: `.../health/DependencyHealthProbe.java`, `.../health/IngestHealthProvider.java`, `.../health/DefaultIngestHealthProvider.java`, `.../service/ConfigHealthProbe.java`

- [ ] **Step 1: Write the probe interface**

`.../health/DependencyHealthProbe.java`:
```java
package com.example.template.health;
import com.example.template.dto.health.ProbeResult;
/** Implemented by each store so HealthService discovers it via List<DependencyHealthProbe>.
 *  Replaces the Python hardcoded _probe(ConfigService/DataService/CacheService) gather. */
public interface DependencyHealthProbe {
    String name();
    ProbeResult probe();
}
```

- [ ] **Step 2: Write the ingest seam**

`.../health/IngestHealthProvider.java`:
```java
package com.example.template.health;
import com.example.template.dto.health.IngestHealth;
public interface IngestHealthProvider {
    IngestHealth currentHealth();
}
```

`.../health/DefaultIngestHealthProvider.java` (used until Phase 4 supplies the real `StreamIngestService`; `@Requires(missingBeans=...)` keeps it from clashing later):
```java
package com.example.template.health;
import com.example.template.dto.health.IngestHealth;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

@Singleton
@Requires(missingBeans = StreamIngestHealthMarker.class)
public class DefaultIngestHealthProvider implements IngestHealthProvider {
    @Override
    public IngestHealth currentHealth() {
        return new IngestHealth("none", "down", false, null, null, 0, false);
    }
}
```

`.../health/StreamIngestHealthMarker.java` (empty marker interface the Phase-4 real provider will implement so the default backs off):
```java
package com.example.template.health;
public interface StreamIngestHealthMarker {}
```

- [ ] **Step 3: Write the Postgres probe**

`.../service/ConfigHealthProbe.java` (mirrors `ConfigService.health_check`; runs `SELECT 1` via the repository's datasource):
```java
package com.example.template.service;

import com.example.template.dto.health.ProbeResult;
import com.example.template.health.DependencyHealthProbe;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;

@Singleton
public class ConfigHealthProbe implements DependencyHealthProbe {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigHealthProbe.class);
    private final DataSource dataSource;

    public ConfigHealthProbe(DataSource dataSource) { this.dataSource = dataSource; }

    @Override
    public String name() { return "postgres"; }

    @Override
    public ProbeResult probe() {
        long start = System.nanoTime();
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.execute("SELECT 1");
            return ProbeResult.up("postgres", ms(start));
        } catch (Exception e) {
            LOG.error("postgres health check failed: {}", e.toString());
            return ProbeResult.down("postgres", ms(start), "unavailable");
        }
    }

    private static double ms(long startNanos) {
        return Math.round((System.nanoTime() - startNanos) / 1_000_000.0 * 100.0) / 100.0;
    }
}
```

- [ ] **Step 4: Compile + commit**

```bash
cd c:/Users/Alexander/templates/java-webservice-template && ./mvnw -q -DskipTests compile
cd c:/Users/Alexander/templates
git add java-webservice-template/src/main/java/com/example/template/health java-webservice-template/src/main/java/com/example/template/service/ConfigHealthProbe.java
git commit -m "feat(java): dependency probe registry, ingest health seam, postgres probe"
```

---

## Task 8: HealthService + HealthController — TDD

**Files:**
- Create: `.../service/HealthService.java`, `.../controller/HealthController.java`
- Test: `.../controller/HealthControllerTest.java`

- [ ] **Step 1: Write the failing test**

`.../controller/HealthControllerTest.java` (reuses the Postgres Testcontainers pattern from Phase 1):
```java
package com.example.template.controller;

import com.example.template.dto.health.DetailedStatusResponse;
import com.example.template.dto.health.LivenessResponse;
import com.example.template.dto.health.ReadinessResponse;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@MicronautTest
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HealthControllerTest implements TestPropertyProvider {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    @Override
    public Map<String, String> getProperties() {
        if (!POSTGRES.isRunning()) POSTGRES.start();
        return Map.of(
            "datasources.default.url", POSTGRES.getJdbcUrl(),
            "datasources.default.username", POSTGRES.getUsername(),
            "datasources.default.password", POSTGRES.getPassword());
    }

    @Inject @Client("/") HttpClient client;

    @Test
    void liveIsAlive() {
        LivenessResponse r = client.toBlocking().retrieve(HttpRequest.GET("/health/live"), LivenessResponse.class);
        assertThat(r.status()).isEqualTo("alive");
        assertThat(r.uptimeSeconds()).isGreaterThanOrEqualTo(0.0);
    }

    @Test
    void readyIsReadyWhenPostgresUp() {
        ReadinessResponse r = client.toBlocking().retrieve(HttpRequest.GET("/health/ready"), ReadinessResponse.class);
        assertThat(r.status()).isEqualTo("ready");
        assertThat(r.checks()).anyMatch(c -> c.name().equals("postgres") && c.status().equals("up"));
    }

    @Test
    void statusReportsAppAndSystem() {
        DetailedStatusResponse r = client.toBlocking().retrieve(HttpRequest.GET("/health/status"), DetailedStatusResponse.class);
        assertThat(r.app().status()).isEqualTo("running");
        assertThat(r.system().host().memoryTotalBytes()).isGreaterThan(0);
        assertThat(r.dependencies()).anyMatch(d -> d.name().equals("postgres"));
    }
}
```

- [ ] **Step 2: Run → fail**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test -Dtest=HealthControllerTest
```
Expected: FAIL (no `/health/*` routes).

- [ ] **Step 3: Write HealthService**

`.../service/HealthService.java`:
```java
package com.example.template.service;

import com.example.template.config.AppSettings;
import com.example.template.core.LastRequestTracker;
import com.example.template.core.SystemMetrics;
import com.example.template.dto.health.*;
import com.example.template.health.DependencyHealthProbe;
import com.example.template.health.IngestHealthProvider;
import jakarta.inject.Singleton;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class HealthService {

    private final AppSettings settings;
    private final List<DependencyHealthProbe> probes;
    private final IngestHealthProvider ingestProvider;
    private final SystemMetrics systemMetrics;
    private final LastRequestTracker lastRequest;
    private final long startMillis = System.currentTimeMillis();

    public HealthService(AppSettings settings, List<DependencyHealthProbe> probes,
                         IngestHealthProvider ingestProvider, SystemMetrics systemMetrics,
                         LastRequestTracker lastRequest) {
        this.settings = settings;
        this.probes = probes;
        this.ingestProvider = ingestProvider;
        this.systemMetrics = systemMetrics;
        this.lastRequest = lastRequest;
    }

    private double uptimeSeconds() {
        return Math.round((System.currentTimeMillis() - startMillis) / 10.0) / 100.0;
    }

    public LivenessResponse liveness() {
        return new LivenessResponse("alive", uptimeSeconds());
    }

    private List<ProbeResult> gather() {
        List<ProbeResult> results = new ArrayList<>();
        for (DependencyHealthProbe p : probes) {
            try {
                results.add(p.probe());
            } catch (Exception e) {
                results.add(ProbeResult.down(p.name(), 0.0, "unavailable"));
            }
        }
        return results;
    }

    public ReadinessResponse readiness() {
        List<ProbeResult> deps = gather();
        IngestHealth ingest = ingestProvider.currentHealth();
        String ingestStatus = "connected".equals(ingest.connectionState()) ? "up" : "down";

        List<CheckResult> checks = new ArrayList<>();
        for (ProbeResult d : deps) {
            checks.add(new CheckResult(d.name(), d.status(), d.latencyMs(), null, null, null, null, null, d.error()));
        }
        checks.add(new CheckResult("ingest", ingestStatus, null, ingest.transport(), ingest.connectionState(),
            ingest.threadAlive(), ingest.lastBatchAt(), ingest.secondsSinceLastBatch(), null));

        boolean allUp = deps.stream().allMatch(d -> d.status().equals("up")) && ingestStatus.equals("up");
        return new ReadinessResponse(allUp ? "ready" : "not_ready", checks);
    }

    public DetailedStatusResponse detailedStatus() {
        List<ProbeResult> deps = gather();
        IngestHealth ingest = ingestProvider.currentHealth();
        double bootSeconds = ManagementFactory.getRuntimeMXBean().getStartTime() / 1000.0;
        return new DetailedStatusResponse(
            new AppInfo(settings.getAppTitle(), settings.getAppVersion(), settings.getStatus()),
            new UptimeInfo(uptimeSeconds(), bootSeconds),
            deps,
            ingest,
            new RequestInfo(lastRequest.lastRequestAt()),
            systemMetrics.snapshot());
    }
}
```

Note: the Phase-2 ingest provider returns `connectionState="down"`, so `/health/ready` would report `ingest` down and overall `not_ready`. To keep the Phase-2 test meaningful, the default provider reports the not-configured ingest as **up** until Phase 4 wires the real transport. Change `DefaultIngestHealthProvider.currentHealth()` to return `connectionState="connected"` **only** in this no-transport default — Phase 4 replaces the bean entirely. Update that record to `new IngestHealth("none", "connected", false, null, null, 0, false)`.

- [ ] **Step 4: Write HealthController**

`.../controller/HealthController.java` (mirrors `routers/health.py`; readiness returns 503 when not ready):
```java
package com.example.template.controller;

import com.example.template.dto.health.DetailedStatusResponse;
import com.example.template.dto.health.LivenessResponse;
import com.example.template.dto.health.ReadinessResponse;
import com.example.template.service.HealthService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;

@Controller("/health")
@ExecuteOn(TaskExecutors.BLOCKING)
public class HealthController {

    private final HealthService health;

    public HealthController(HealthService health) { this.health = health; }

    @Get("/live")
    public LivenessResponse live() { return health.liveness(); }

    @Get("/ready")
    public HttpResponse<ReadinessResponse> ready() {
        ReadinessResponse r = health.readiness();
        return r.status().equals("ready") ? HttpResponse.ok(r) : HttpResponse.status(io.micronaut.http.HttpStatus.SERVICE_UNAVAILABLE).body(r);
    }

    @Get("/status")
    public DetailedStatusResponse status() { return health.detailedStatus(); }
}
```

- [ ] **Step 5: Run → pass**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test -Dtest=HealthControllerTest
```
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
cd c:/Users/Alexander/templates
git add java-webservice-template/src/main/java/com/example/template/service/HealthService.java java-webservice-template/src/main/java/com/example/template/controller/HealthController.java java-webservice-template/src/main/java/com/example/template/health/DefaultIngestHealthProvider.java java-webservice-template/src/test/java/com/example/template/controller/HealthControllerTest.java
git commit -m "feat(java): health service + /health/{live,ready,status} endpoints"
```

---

## Task 9: Prometheus metrics endpoint — TDD

**Files:**
- Create: `.../observability/MetricsBinder.java`, `.../observability/MetricsController.java`
- Test: `.../observability/MetricsControllerTest.java`

> **Divergence note:** Java uses Micrometer's standard JVM/processor/uptime meter binders (configured in Task 2) for process/host stats instead of hand-built gauges; only the dependency/ingest gauges are custom. This is the idiomatic Micrometer approach and reduces hand-rolled code versus the Python `MetricsService`.

- [ ] **Step 1: Write the failing test**

`.../observability/MetricsControllerTest.java`:
```java
package com.example.template.observability;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@MicronautTest
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MetricsControllerTest implements TestPropertyProvider {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    @Override
    public Map<String, String> getProperties() {
        if (!POSTGRES.isRunning()) POSTGRES.start();
        return Map.of(
            "datasources.default.url", POSTGRES.getJdbcUrl(),
            "datasources.default.username", POSTGRES.getUsername(),
            "datasources.default.password", POSTGRES.getPassword());
    }

    @Inject @Client("/") HttpClient client;

    @Test
    void metricsExposesPrometheusTextWithCustomGauges() {
        String body = client.toBlocking().retrieve(HttpRequest.GET("/metrics"));
        assertThat(body).contains("dependency_up");
        assertThat(body).contains("jvm_memory_used_bytes"); // standard Micrometer binder
    }
}
```

- [ ] **Step 2: Run → fail**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test -Dtest=MetricsControllerTest
```
Expected: FAIL (no `/metrics` route).

- [ ] **Step 3: Write MetricsBinder + MetricsController**

`.../observability/MetricsBinder.java`:
```java
package com.example.template.observability;

import com.example.template.service.HealthService;
import com.example.template.dto.health.ProbeResult;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import jakarta.inject.Singleton;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicDouble;

/** Registers and refreshes custom gauges from the health snapshot. Mirrors the
 *  Python MetricsService.refresh() pull model. */
@Singleton
public class MetricsBinder {

    private final MeterRegistry registry;
    private final HealthService health;
    private final ConcurrentHashMap<String, AtomicDouble> values = new ConcurrentHashMap<>();

    public MetricsBinder(MeterRegistry registry, HealthService health) {
        this.registry = registry;
        this.health = health;
    }

    private AtomicDouble gauge(String name, Tags tags, String key) {
        return values.computeIfAbsent(key, k -> {
            AtomicDouble v = new AtomicDouble(0.0);
            registry.gauge(name, tags, v, AtomicDouble::get);
            return v;
        });
    }

    public void refresh() {
        var status = health.detailedStatus();
        for (ProbeResult dep : status.dependencies()) {
            gauge("dependency_up", Tags.of("name", dep.name()), "up:" + dep.name())
                .set(dep.status().equals("up") ? 1.0 : 0.0);
            gauge("dependency_check_latency_seconds", Tags.of("name", dep.name()), "lat:" + dep.name())
                .set(dep.latencyMs() / 1000.0);
        }
        var ingest = status.ingest();
        gauge("ingest_rows_ingested", Tags.empty(), "ingest_rows").set(ingest.rowsIngestedTotal());
        Double secs = ingest.secondsSinceLastBatch();
        gauge("ingest_seconds_since_last_batch", Tags.empty(), "ingest_secs").set(secs == null ? Double.NaN : secs);
    }
}
```

Add a tiny `AtomicDouble` if not on the classpath — Micrometer ships `io.micrometer.core.instrument.internal.DefaultGauge`, but the simplest is Guava's `AtomicDouble`. To avoid a Guava dependency, replace `AtomicDouble` with `java.util.concurrent.atomic.DoubleAdder` reset semantics, or a small wrapper. Use this minimal wrapper instead — create `.../observability/MutableDouble.java`:
```java
package com.example.template.observability;
public final class MutableDouble {
    private volatile double value;
    public void set(double v) { this.value = v; }
    public double get() { return value; }
}
```
and substitute `MutableDouble` for `AtomicDouble` in `MetricsBinder` (`registry.gauge(name, tags, v, MutableDouble::get)`).

`.../observability/MetricsController.java`:
```java
package com.example.template.observability;

import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;

@Controller("/metrics")
@ExecuteOn(TaskExecutors.BLOCKING)
public class MetricsController {

    private final PrometheusMeterRegistry registry;
    private final MetricsBinder binder;

    public MetricsController(PrometheusMeterRegistry registry, MetricsBinder binder) {
        this.registry = registry;
        this.binder = binder;
    }

    @Get
    @Produces(MediaType.TEXT_PLAIN)
    public String scrape() {
        binder.refresh();
        return registry.scrape();
    }
}
```

- [ ] **Step 4: Run → pass**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test -Dtest=MetricsControllerTest
```
Expected: PASS. (If `PrometheusMeterRegistry` injection requires the `micronaut-micrometer-registry-prometheus` registry bean to be present, it is — added in Task 1.)

- [ ] **Step 5: Commit**

```bash
cd c:/Users/Alexander/templates
git add java-webservice-template/src/main/java/com/example/template/observability java-webservice-template/src/test/java/com/example/template/observability
git commit -m "feat(java): /metrics endpoint with custom dependency gauges"
```

---

## Task 10: Body-size limit test + full suite green

**Files:**
- Test: `.../core/BodySizeTest.java`

- [ ] **Step 1: Write the test (config already set in Task 2)**

`.../core/BodySizeTest.java` — verifies Micronaut's `max-request-size` returns 413 (the `MaxBodySizeMiddleware` analogue). Lower the limit for the test via property:
```java
package com.example.template.core;

import io.micronaut.context.annotation.Property;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

@MicronautTest
@Property(name = "micronaut.server.max-request-size", value = "100")
class BodySizeTest {

    @Controller("/__bodytest")
    static class Sink {
        @Post(consumes = MediaType.TEXT_PLAIN)
        String accept(@Body String body) { return "len=" + body.length(); }
    }

    @Inject @Client("/") HttpClient client;

    @Test
    void oversizedBodyRejectedWith413() {
        String big = "x".repeat(500);
        HttpClientResponseException ex = catchThrowableOfType(
            () -> client.toBlocking().exchange(
                HttpRequest.POST("/__bodytest", big).contentType(MediaType.TEXT_PLAIN)),
            HttpClientResponseException.class);
        assertThat(ex.getStatus()).isEqualTo(HttpStatus.REQUEST_ENTITY_TOO_LARGE);
    }
}
```

- [ ] **Step 2: Run → pass**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test -Dtest=BodySizeTest
```
Expected: PASS.

- [ ] **Step 3: Run the FULL suite**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test
```
Expected: PASS — all Phase 1 + Phase 2 tests.

- [ ] **Step 4: Commit**

```bash
cd c:/Users/Alexander/templates
git add java-webservice-template/src/test/java/com/example/template/core/BodySizeTest.java
git commit -m "test(java): inbound body-size 413 limit"
```

---

## Task 11: Root info endpoint + OpenAPI/Swagger UI — TDD

Mirrors the Python `main.py` root (`GET /` → title/version/docs/MCP) and FastAPI's auto `/docs`. Micronaut OpenAPI generates the spec at compile time and serves Swagger UI; because it scans controllers at build, adding it now means every controller from later phases is included automatically.

**Files:**
- Modify: `pom.xml` (OpenAPI processor + swagger annotations), `application.yml` (swagger static resources), `Application.java` (`@OpenAPIDefinition`)
- Create: `.../dto/RootInfo.java`, `.../controller/RootController.java`, `src/main/resources/META-INF/openapi.properties`
- Test: `.../controller/RootControllerTest.java`

- [ ] **Step 1: Add OpenAPI dependencies to pom.xml**

```xml
<dependency><groupId>io.micronaut.openapi</groupId><artifactId>micronaut-openapi-annotations</artifactId><scope>compile</scope></dependency>
<dependency><groupId>io.swagger.core.v3</groupId><artifactId>swagger-annotations</artifactId><scope>compile</scope></dependency>
```
Add `io.micronaut.openapi:micronaut-openapi` to the `annotationProcessorPaths` of the compiler/Micronaut plugin (the Micronaut Maven parent recognises it). Enable the Swagger UI view via `src/main/resources/META-INF/openapi.properties`:
```properties
micronaut.openapi.views.spec=swagger-ui.enabled=true,swagger-ui.theme=flattop
```

- [ ] **Step 2: Add Swagger static-resource routes to application.yml**

Under `micronaut:` add:
```yaml
  router:
    static-resources:
      swagger:
        paths: classpath:META-INF/swagger
        mapping: /swagger/**
      swagger-ui:
        paths: classpath:META-INF/swagger/views/swagger-ui
        mapping: /swagger-ui/**
```

- [ ] **Step 3: Annotate Application with @OpenAPIDefinition**

In `.../Application.java`, add above the class:
```java
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;

@OpenAPIDefinition(info = @Info(title = "Template Micronaut Service", version = "1.0.0",
    description = "A Java Micronaut service mirroring the Python webservice template"))
```

- [ ] **Step 4: Write the failing test**

`.../controller/RootControllerTest.java`:
```java
package com.example.template.controller;

import com.example.template.dto.RootInfo;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@MicronautTest
class RootControllerTest {

    @Inject @Client("/") HttpClient client;

    @Test
    void rootReturnsServiceInfo() {
        RootInfo info = client.toBlocking().retrieve(HttpRequest.GET("/"), RootInfo.class);
        assertThat(info.title()).isNotBlank();
        assertThat(info.docs()).isEqualTo("/swagger-ui");
        assertThat(info.mcp()).isEqualTo("/mcp");
    }
}
```

- [ ] **Step 5: Run → fail**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test -Dtest=RootControllerTest
```
Expected: FAIL (compile — `RootInfo` missing / no `/` route).

- [ ] **Step 6: Implement RootInfo + RootController**

`.../dto/RootInfo.java`:
```java
package com.example.template.dto;
import io.micronaut.serde.annotation.Serdeable;
@Serdeable
public record RootInfo(String title, String version, String description, String docs, String mcp) {}
```

`.../controller/RootController.java` (mirrors the Python `get_root`):
```java
package com.example.template.controller;

import com.example.template.config.AppSettings;
import com.example.template.dto.RootInfo;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;

@Controller("/")
public class RootController {

    private final AppSettings settings;

    public RootController(AppSettings settings) { this.settings = settings; }

    @Get
    public RootInfo root() {
        return new RootInfo(settings.getAppTitle(), settings.getAppVersion(),
            settings.getAppDescription(), "/swagger-ui", "/mcp");
    }
}
```

- [ ] **Step 7: Run → pass**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test -Dtest=RootControllerTest
```
Expected: PASS.

- [ ] **Step 8: Verify Swagger UI serves (manual)**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q -DskipTests package
# The generated OpenAPI yaml lands under target/classes/META-INF/swagger/. Confirm it exists:
ls target/classes/META-INF/swagger/
```
Expected: a `*.yml` spec file present (served at `/swagger/<name>.yml`, with the UI at `/swagger-ui`). If the view isn't generated, confirm `openapi.properties` and the `micronaut-openapi` processor path are in place per the Micronaut OpenAPI docs.

- [ ] **Step 9: Commit**

```bash
cd c:/Users/Alexander/templates
git add java-webservice-template/pom.xml java-webservice-template/src/main/resources/application.yml java-webservice-template/src/main/resources/META-INF/openapi.properties java-webservice-template/src/main/java/com/example/template/Application.java java-webservice-template/src/main/java/com/example/template/dto/RootInfo.java java-webservice-template/src/main/java/com/example/template/controller/RootController.java java-webservice-template/src/test/java/com/example/template/controller/RootControllerTest.java
git commit -m "feat(java): root info endpoint + OpenAPI/Swagger UI"
```

---

## Phase 2 self-review checklist
- [ ] Correlation id is generated/adopted, echoed, and visible in MDC on the handler thread (`CorrelationFilterTest` green).
- [ ] `Server-Timing` header carries `<boundary>;dur=` + `total;dur=` (`ServerTimingFilterTest` green).
- [ ] `/health/{live,ready,status}` mirror the Python JSON shapes; `/ready` returns 503 when a dependency is down.
- [ ] `/metrics` serves Prometheus text including `dependency_up` and standard JVM binders.
- [ ] Oversized body → 413.
- [ ] `GET /` returns service info (title/version/docs/MCP); Swagger UI is served at `/swagger-ui`.
- [ ] Probe registry pattern is in place so Phases 3–4 stores self-register with no `HealthService` edit.

## Deferred to later phases
- ClickHouse/Redis probes auto-join the registry → Phase 3.
- Real ingest health (replaces `DefaultIngestHealthProvider`) → Phase 4.
- `timed(...)` boundaries inside `ConfigService` queries — add when wiring stores in Phase 3 (the `Timed` helper exists now).
