# Java Webservice Template — Phase 4: Stream Ingestion + Simplified LSM Store

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the streaming ingestion path and the **simplified append-only** LSM store: `POST /data/ingest` (Arrow IPC → 202), `GET /data/cache` (rows with `seqno`+`op` for client-side compaction), a dedicated ingest thread consuming a Flight or Solace transport with backoff/watchdog/SIGTERM self-recovery, and the real ingest-health provider. Mirrors `python-webservice-template/{persistence/stream_store,services/stream_ingest,ingestion/*,core/retry}` — with the deliberate simplification from the spec: **no window-function merge; the client compacts.**

**Architecture:** `LsmStore` is single-writer and append-only — `ingest()` assigns a monotonic `seqno` per row and appends to an immutable snapshot swapped via a `volatile` reference; `query()` returns rows in `seqno` order carrying `seqno`+`op`. `StreamIngestService` owns a platform thread running the consume loop (jittered exponential backoff, consecutive-failure → `SIGTERM`) and a scheduled disconnect watchdog. A `BatchConsumer` (Flight or Solace, selected by config) feeds Arrow `VectorSchemaRoot` batches; `ArrowDecoder` turns them into rows. The service implements `IngestHealthProvider` + `StreamIngestHealthMarker`, so the Phase-2 default provider backs off and real ingest health flows into `/health`.

**Tech Stack:** Apache Arrow Java (`arrow-vector`, `arrow-memory-netty`, `flight-core`), Solace PubSub+ Java API (`solace-messaging-client`), Testcontainers (Solace).

**Reference:** `persistence/stream_store/lsm_store.py`, `services/stream_ingest.py`, `ingestion/base.py`, `ingestion/flight/client.py`, `ingestion/solace/client.py`, `core/retry.py`, `routers/data.py` (cache+ingest), `schemas/data.py`.

**Conventions:** as prior phases.

---

## File structure produced by this phase

| File | Responsibility |
|---|---|
| `pom.xml` | + arrow-vector, arrow-memory-netty, flight-core, solace-messaging-client; + testcontainers-solace |
| `application.yml` | + `template.ingest.*` config |
| `.../config/IngestSettings.java` | Ingest/transport config |
| `.../ingestion/ConnectionState.java` | enum (connected/reconnecting/down) |
| `.../ingestion/BatchConsumer.java` | consumer interface |
| `.../ingestion/ArrowDecoder.java` | VectorSchemaRoot / IPC bytes → rows |
| `.../persistence/streamstore/LsmRow.java` | id/name/value/seqno/op row |
| `.../persistence/streamstore/LsmStore.java` | append-only single-writer store |
| `.../dto/CachedDataRow.java`, `.../dto/CachedDataRowsResponse.java` | `/data/cache` response (seqno+op) |
| `.../core/Retry.java` | `connectWithBackoff` |
| `.../service/StreamIngestService.java` | ingest thread + watchdog + ingest health |
| `.../ingestion/flight/FlightBatchConsumer.java` | Arrow Flight transport |
| `.../ingestion/solace/SolaceBatchConsumer.java` | Solace transport |
| `.../ingestion/ConsumerFactory.java` | transport selection |
| DataController additions: `/data/cache`, `/data/ingest` |
| Tests: LSM unit, HTTP ingest, Flight consumer (embedded server), Solace (Testcontainers) |

---

## Task 1: Ingestion dependencies + config

**Files:** Modify `pom.xml`, `application.yml`; create `.../config/IngestSettings.java`.

- [ ] **Step 1: Add dependencies**

```xml
<dependency><groupId>org.apache.arrow</groupId><artifactId>arrow-vector</artifactId><version>18.1.0</version><scope>compile</scope></dependency>
<dependency><groupId>org.apache.arrow</groupId><artifactId>arrow-memory-netty</artifactId><version>18.1.0</version><scope>runtime</scope></dependency>
<dependency><groupId>org.apache.arrow</groupId><artifactId>flight-core</artifactId><version>18.1.0</version><scope>compile</scope></dependency>
<dependency><groupId>com.solace</groupId><artifactId>solace-messaging-client</artifactId><version>1.10.0</version><scope>compile</scope></dependency>
<dependency><groupId>org.testcontainers</groupId><artifactId>solace</artifactId><scope>test</scope></dependency>
```

Arrow on Java 25 requires the JVM flag `--add-opens=java.base/java.nio=ALL-UNNAMED`. Add to the surefire/exec config in `pom.xml` `<build><plugins>`:

```xml
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-surefire-plugin</artifactId>
  <configuration>
    <argLine>--add-opens=java.base/java.nio=ALL-UNNAMED --enable-native-access=ALL-UNNAMED</argLine>
  </configuration>
</plugin>
```

(The same flag goes into the runtime `JAVA_TOOL_OPTIONS` in the Phase-5 Dockerfile.)

- [ ] **Step 2: Add ingest config to application.yml**

```yaml
template:
  ingest:
    transport: ${INGEST_TRANSPORT:flight}   # flight | solace
    max-batch-bytes: 16777216
    max-disconnect-seconds: 60
    staleness-threshold-seconds: 0           # 0 = never report stale
    flight:
      host: ${FLIGHT_HOST:localhost}
      port: ${FLIGHT_PORT:8815}
      ticket: ${FLIGHT_TICKET:items}
    solace:
      host: ${SOLACE_HOST:localhost}
      port: ${SOLACE_PORT:55555}
      vpn: ${SOLACE_VPN:default}
      username: ${SOLACE_USERNAME:admin}
      password: ${SOLACE_PASSWORD:admin}
      topic: ${SOLACE_TOPIC:ingest/batches}
```

- [ ] **Step 3: Write IngestSettings**

`.../config/IngestSettings.java`:
```java
package com.example.template.config;

import io.micronaut.context.annotation.ConfigurationProperties;

@ConfigurationProperties("template.ingest")
public class IngestSettings {
    private String transport = "flight";
    private long maxBatchBytes = 16L * 1024 * 1024;
    private long maxDisconnectSeconds = 60;
    private double stalenessThresholdSeconds = 0;
    private Flight flight = new Flight();
    private Solace solace = new Solace();

    public String getTransport() { return transport; }
    public void setTransport(String v) { this.transport = v; }
    public long getMaxBatchBytes() { return maxBatchBytes; }
    public void setMaxBatchBytes(long v) { this.maxBatchBytes = v; }
    public long getMaxDisconnectSeconds() { return maxDisconnectSeconds; }
    public void setMaxDisconnectSeconds(long v) { this.maxDisconnectSeconds = v; }
    public double getStalenessThresholdSeconds() { return stalenessThresholdSeconds; }
    public void setStalenessThresholdSeconds(double v) { this.stalenessThresholdSeconds = v; }
    public Flight getFlight() { return flight; }
    public void setFlight(Flight v) { this.flight = v; }
    public Solace getSolace() { return solace; }
    public void setSolace(Solace v) { this.solace = v; }

    @ConfigurationProperties("flight")
    public static class Flight {
        private String host = "localhost";
        private int port = 8815;
        private String ticket = "items";
        public String getHost() { return host; } public void setHost(String v) { this.host = v; }
        public int getPort() { return port; } public void setPort(int v) { this.port = v; }
        public String getTicket() { return ticket; } public void setTicket(String v) { this.ticket = v; }
    }

    @ConfigurationProperties("solace")
    public static class Solace {
        private String host = "localhost"; private int port = 55555; private String vpn = "default";
        private String username = "admin"; private String password = "admin"; private String topic = "ingest/batches";
        public String getHost() { return host; } public void setHost(String v) { this.host = v; }
        public int getPort() { return port; } public void setPort(int v) { this.port = v; }
        public String getVpn() { return vpn; } public void setVpn(String v) { this.vpn = v; }
        public String getUsername() { return username; } public void setUsername(String v) { this.username = v; }
        public String getPassword() { return password; } public void setPassword(String v) { this.password = v; }
        public String getTopic() { return topic; } public void setTopic(String v) { this.topic = v; }
    }
}
```

- [ ] **Step 4: Compile + commit**

```bash
cd c:/Users/Alexander/templates/java-webservice-template && ./mvnw -q -DskipTests compile
cd c:/Users/Alexander/templates
git add java-webservice-template/pom.xml java-webservice-template/src/main/resources/application.yml java-webservice-template/src/main/java/com/example/template/config/IngestSettings.java
git commit -m "build(java): arrow/flight/solace deps + ingest config"
```

---

## Task 2: ConnectionState, BatchConsumer, LSM row + DTOs

**Files:**
- Create: `.../ingestion/ConnectionState.java`, `.../ingestion/BatchConsumer.java`
- Create: `.../persistence/streamstore/LsmRow.java`
- Create: `.../dto/CachedDataRow.java`, `.../dto/CachedDataRowsResponse.java`

- [ ] **Step 1: Write the enum and interface** (mirror `ingestion/base.py`)

`.../ingestion/ConnectionState.java`:
```java
package com.example.template.ingestion;
public enum ConnectionState {
    CONNECTED("connected"), RECONNECTING("reconnecting"), DOWN("down");
    private final String value;
    ConnectionState(String value) { this.value = value; }
    public String value() { return value; }
}
```

`.../ingestion/BatchConsumer.java`. A blocking consumer run on the ingest thread; `batches()` yields decoded rows (Java has no generator, so we use a callback the loop drives):
```java
package com.example.template.ingestion;

import com.example.template.persistence.streamstore.LsmRow;
import java.util.List;
import java.util.function.Consumer;

/** Synchronous interface run on the dedicated ingest thread. consume() blocks,
 *  delivering decoded row batches to the sink until close() unblocks it. */
public interface BatchConsumer extends AutoCloseable {
    /** Block, delivering each decoded batch (list of rows) to {@code sink}.
     *  Returns when the stream ends cleanly (e.g. close()). Throws on transport failure. */
    void consume(Consumer<List<LsmRow>> sink) throws Exception;

    @Override
    void close();

    ConnectionState connectionState();
}
```

- [ ] **Step 2: Write the LSM row**

`.../persistence/streamstore/LsmRow.java`:
```java
package com.example.template.persistence.streamstore;
public record LsmRow(long id, String name, String value, long seqno, String op) {}
```

- [ ] **Step 3: Write the cache DTOs** (the spec's seqno+op divergence)

`.../dto/CachedDataRow.java`:
```java
package com.example.template.dto;
import io.micronaut.serde.annotation.Serdeable;
@Serdeable
public record CachedDataRow(long id, String name, String value, long seqno, String op) {}
```

`.../dto/CachedDataRowsResponse.java`:
```java
package com.example.template.dto;
import io.micronaut.serde.annotation.Serdeable;
import java.util.List;
@Serdeable
public record CachedDataRowsResponse(List<CachedDataRow> rows, long total, int limit) {}
```

- [ ] **Step 4: Compile + commit**

```bash
cd c:/Users/Alexander/templates/java-webservice-template && ./mvnw -q -DskipTests compile
cd c:/Users/Alexander/templates
git add java-webservice-template/src/main/java/com/example/template/ingestion/ConnectionState.java java-webservice-template/src/main/java/com/example/template/ingestion/BatchConsumer.java java-webservice-template/src/main/java/com/example/template/persistence/streamstore/LsmRow.java java-webservice-template/src/main/java/com/example/template/dto/CachedDataRow.java java-webservice-template/src/main/java/com/example/template/dto/CachedDataRowsResponse.java
git commit -m "feat(java): connection-state, batch-consumer iface, LSM row + cache DTOs"
```

---

## Task 3: Simplified append-only LSM store — TDD

**Files:**
- Create: `.../persistence/streamstore/LsmStore.java`
- Test: `.../persistence/streamstore/LsmStoreTest.java`

- [ ] **Step 1: Write the failing test**

`.../persistence/streamstore/LsmStoreTest.java`:
```java
package com.example.template.persistence.streamstore;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class LsmStoreTest {

    @Test
    void appendsAssignSeqnoAndQueryReturnsInOrderWithOpAndSeqno() {
        LsmStore store = new LsmStore();
        store.ingest(List.of(new LsmRow(1, "a", "x", 0, "insert"),
                             new LsmRow(2, "b", "y", 0, "insert")));
        store.ingest(List.of(new LsmRow(1, "a", "x2", 0, "insert")));

        LsmStore.QueryResult r = store.query(10);
        assertThat(r.total()).isEqualTo(3);           // append-only: no dedup
        assertThat(r.rows()).extracting(LsmRow::seqno).containsExactly(0L, 1L, 2L);
        assertThat(r.rows()).extracting(LsmRow::id).containsExactly(1L, 2L, 1L);
        assertThat(r.rows().get(2).value()).isEqualTo("x2");
    }

    @Test
    void queryRespectsLimit() {
        LsmStore store = new LsmStore();
        for (int i = 0; i < 5; i++) store.ingest(List.of(new LsmRow(i, "n", "v", 0, "insert")));
        LsmStore.QueryResult r = store.query(2);
        assertThat(r.rows()).hasSize(2);
        assertThat(r.total()).isEqualTo(5);
    }

    @Test
    void preservesDeleteTombstonesAsPlainRows() {
        LsmStore store = new LsmStore();
        store.ingest(List.of(new LsmRow(1, "a", "x", 0, "insert"),
                             new LsmRow(1, "a", "x", 0, "delete")));
        LsmStore.QueryResult r = store.query(10);
        assertThat(r.rows()).extracting(LsmRow::op).containsExactly("insert", "delete");
    }
}
```

- [ ] **Step 2: Run → fail**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test -Dtest=LsmStoreTest
```
Expected: FAIL (compile — `LsmStore` missing).

- [ ] **Step 3: Implement LsmStore**

`.../persistence/streamstore/LsmStore.java`:
```java
package com.example.template.persistence.streamstore;

import java.util.ArrayList;
import java.util.List;

/**
 * Simplified append-only stream store.
 *
 * <p>SIMPLIFIED FROM THE PYTHON TEMPLATE: the polars window-function merge /
 * compaction is removed. Rows are appended with a monotonic seqno and returned
 * as-is (duplicates and delete tombstones included); the client receiving the
 * data performs compaction.
 *
 * <p>SINGLE-WRITER CONTRACT: exactly one thread (the ingest consumer thread)
 * calls {@link #ingest}. Readers ({@link #query}) are lock-free: each append
 * publishes a new immutable snapshot via the {@code volatile} reference, so a
 * reader always sees a consistent list.
 */
public final class LsmStore {

    public record QueryResult(List<LsmRow> rows, long total) {}

    private volatile List<LsmRow> snapshot = List.of();
    private long seqno = 0; // writer-private

    public void ingest(List<LsmRow> batch) {
        List<LsmRow> next = new ArrayList<>(snapshot);
        for (LsmRow row : batch) {
            next.add(new LsmRow(row.id(), row.name(), row.value(), seqno++, row.op()));
        }
        snapshot = List.copyOf(next); // atomic publish of immutable snapshot
    }

    public QueryResult query(int limit) {
        List<LsmRow> snap = snapshot; // atomic read
        long total = snap.size();
        List<LsmRow> rows = limit < snap.size() ? List.copyOf(snap.subList(0, limit)) : snap;
        return new QueryResult(rows, total);
    }
}
```

- [ ] **Step 4: Run → pass**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test -Dtest=LsmStoreTest
```
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
cd c:/Users/Alexander/templates
git add java-webservice-template/src/main/java/com/example/template/persistence/streamstore/LsmStore.java java-webservice-template/src/test/java/com/example/template/persistence/streamstore/LsmStoreTest.java
git commit -m "feat(java): simplified append-only LSM store (client-side compaction)"
```

---

## Task 4: Arrow decoder — TDD

**Files:**
- Create: `.../ingestion/ArrowDecoder.java`
- Test: `.../ingestion/ArrowDecoderTest.java`

- [ ] **Step 1: Write the failing test** (builds an Arrow IPC stream, decodes it)

`.../ingestion/ArrowDecoderTest.java`:
```java
package com.example.template.ingestion;

import com.example.template.persistence.streamstore.LsmRow;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ArrowDecoderTest {

    @Test
    void decodesIpcBytesToRows() throws Exception {
        byte[] ipc;
        try (RootAllocator allocator = new RootAllocator()) {
            Schema schema = new Schema(List.of(
                new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.notNullable(new ArrowType.Utf8()), null),
                new Field("value", FieldType.notNullable(new ArrowType.Utf8()), null),
                new Field("op", FieldType.notNullable(new ArrowType.Utf8()), null)));
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                BigIntVector id = (BigIntVector) root.getVector("id");
                VarCharVector name = (VarCharVector) root.getVector("name");
                VarCharVector value = (VarCharVector) root.getVector("value");
                VarCharVector op = (VarCharVector) root.getVector("op");
                id.allocateNew(2); name.allocateNew(); value.allocateNew(); op.allocateNew();
                id.set(0, 1); name.setSafe(0, "a".getBytes(StandardCharsets.UTF_8));
                value.setSafe(0, "x".getBytes(StandardCharsets.UTF_8)); op.setSafe(0, "insert".getBytes(StandardCharsets.UTF_8));
                id.set(1, 2); name.setSafe(1, "b".getBytes(StandardCharsets.UTF_8));
                value.setSafe(1, "y".getBytes(StandardCharsets.UTF_8)); op.setSafe(1, "delete".getBytes(StandardCharsets.UTF_8));
                root.setRowCount(2);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                try (ArrowStreamWriter w = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
                    w.start(); w.writeBatch(); w.end();
                }
                ipc = out.toByteArray();
            }
        }

        List<LsmRow> rows;
        try (ArrowDecoder decoder = new ArrowDecoder()) {
            rows = decoder.decodeAll(ipc);
        }
        assertThat(rows).extracting(LsmRow::id).containsExactly(1L, 2L);
        assertThat(rows).extracting(LsmRow::op).containsExactly("insert", "delete");
        assertThat(rows.get(1).value()).isEqualTo("y");
    }
}
```

- [ ] **Step 2: Run → fail**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test -Dtest=ArrowDecoderTest
```
Expected: FAIL (compile — `ArrowDecoder` missing).

- [ ] **Step 3: Implement ArrowDecoder**

`.../ingestion/ArrowDecoder.java`:
```java
package com.example.template.ingestion;

import com.example.template.persistence.streamstore.LsmRow;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Decodes Arrow IPC stream bytes (and live VectorSchemaRoots) into LSM rows.
 *  Expects columns id (int64), name, value, op (utf8). */
public final class ArrowDecoder implements AutoCloseable {

    private final RootAllocator allocator = new RootAllocator();

    public List<LsmRow> decodeAll(byte[] ipcBytes) throws Exception {
        List<LsmRow> rows = new ArrayList<>();
        try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipcBytes), allocator)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                rows.addAll(decodeRoot(root));
            }
        }
        return rows;
    }

    public static List<LsmRow> decodeRoot(VectorSchemaRoot root) {
        BigIntVector id = (BigIntVector) root.getVector("id");
        VarCharVector name = (VarCharVector) root.getVector("name");
        VarCharVector value = (VarCharVector) root.getVector("value");
        VarCharVector op = (VarCharVector) root.getVector("op");
        int n = root.getRowCount();
        List<LsmRow> rows = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            rows.add(new LsmRow(
                id.get(i),
                new String(name.get(i), StandardCharsets.UTF_8),
                new String(value.get(i), StandardCharsets.UTF_8),
                0,
                new String(op.get(i), StandardCharsets.UTF_8)));
        }
        return rows;
    }

    @Override
    public void close() {
        allocator.close();
    }
}
```

- [ ] **Step 4: Run → pass**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test -Dtest=ArrowDecoderTest
```
Expected: PASS. (If Arrow throws a `nio` access error, confirm the surefire `--add-opens` argLine from Task 1 is present.)

- [ ] **Step 5: Commit**

```bash
cd c:/Users/Alexander/templates
git add java-webservice-template/src/main/java/com/example/template/ingestion/ArrowDecoder.java java-webservice-template/src/test/java/com/example/template/ingestion/ArrowDecoderTest.java
git commit -m "feat(java): Arrow IPC decoder"
```

---

## Task 5: Retry utility — TDD

**Files:**
- Create: `.../core/Retry.java`
- Test: `.../core/RetryTest.java`

- [ ] **Step 1: Write the failing test**

`.../core/RetryTest.java`:
```java
package com.example.template.core;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetryTest {

    @Test
    void succeedsAfterTransientFailures() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        String result = Retry.connectWithBackoff(() -> {
            if (attempts.incrementAndGet() < 3) throw new RuntimeException("boom");
            return "ok";
        }, "test", 5, 0.001, 0.01);
        assertThat(result).isEqualTo("ok");
        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    void propagatesAfterMaxAttempts() {
        AtomicInteger attempts = new AtomicInteger();
        assertThatThrownBy(() -> Retry.connectWithBackoff(() -> {
            attempts.incrementAndGet();
            throw new RuntimeException("always");
        }, "test", 3, 0.001, 0.01)).isInstanceOf(RuntimeException.class);
        assertThat(attempts.get()).isEqualTo(3);
    }
}
```

- [ ] **Step 2: Run → fail**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test -Dtest=RetryTest
```
Expected: FAIL (compile — `Retry` missing).

- [ ] **Step 3: Implement Retry** (mirrors `core/retry.py`)

`.../core/Retry.java`:
```java
package com.example.template.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

public final class Retry {
    private static final Logger LOG = LoggerFactory.getLogger(Retry.class);

    private Retry() {}

    /** Calls {@code connect} with randomised exponential backoff (25% jitter).
     *  After {@code maxAttempts} consecutive failures the last exception propagates. */
    public static <T> T connectWithBackoff(Callable<T> connect, String label,
                                           int maxAttempts, double baseDelaySeconds, double maxDelaySeconds) throws Exception {
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return connect.call();
            } catch (Exception e) {
                if (attempt == maxAttempts) {
                    LOG.error("{}: all {} connection attempts failed", label, maxAttempts);
                    throw e;
                }
                double delay = Math.min(baseDelaySeconds * Math.pow(2, attempt - 1), maxDelaySeconds);
                double jitter = delay * 0.25 * ThreadLocalRandom.current().nextDouble();
                LOG.warn("{}: attempt {}/{} failed – retrying in {}s: {}", label, attempt, maxAttempts,
                    String.format("%.3f", delay + jitter), e.toString());
                Thread.sleep((long) ((delay + jitter) * 1000));
            }
        }
        throw new IllegalStateException("unreachable");
    }
}
```

- [ ] **Step 4: Run → pass + commit**

```bash
cd c:/Users/Alexander/templates/java-webservice-template && ./mvnw -q test -Dtest=RetryTest
cd c:/Users/Alexander/templates
git add java-webservice-template/src/main/java/com/example/template/core/Retry.java java-webservice-template/src/test/java/com/example/template/core/RetryTest.java
git commit -m "feat(java): connect-with-backoff retry utility"
```

---

## Task 5B: Fail-fast startup smoke-test (resilience) — TDD

Mirrors the Python "eager smoke-test on startup, retry with backoff, abort after N failures" guarantee. Reuses the `DependencyHealthProbe` beans (Postgres/ClickHouse/Redis) created in Phases 1–3 and the `Retry` util from Task 5. Disabled under the test environment so per-test containers (which provision only the store under test) don't trip it.

**Files:**
- Modify: `.../config/AppSettings.java` (add connect-retry fields)
- Create: `.../core/StartupSmoke.java`
- Test: `.../core/StartupSmokeTest.java`

- [ ] **Step 1: Add connect-retry fields to AppSettings**

In `.../config/AppSettings.java` add three fields + getters/setters (mirrors the Python `connect_*` settings):
```java
    private int connectMaxAttempts = 5;
    private double connectBaseDelaySeconds = 1.0;
    private double connectMaxDelaySeconds = 30.0;

    public int getConnectMaxAttempts() { return connectMaxAttempts; }
    public void setConnectMaxAttempts(int v) { this.connectMaxAttempts = v; }
    public double getConnectBaseDelaySeconds() { return connectBaseDelaySeconds; }
    public void setConnectBaseDelaySeconds(double v) { this.connectBaseDelaySeconds = v; }
    public double getConnectMaxDelaySeconds() { return connectMaxDelaySeconds; }
    public void setConnectMaxDelaySeconds(double v) { this.connectMaxDelaySeconds = v; }
```

- [ ] **Step 2: Write the failing test** (unit-level: a down probe must make the smoke-test throw, proving fail-fast)

`.../core/StartupSmokeTest.java`:
```java
package com.example.template.core;

import com.example.template.config.AppSettings;
import com.example.template.dto.health.ProbeResult;
import com.example.template.health.DependencyHealthProbe;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StartupSmokeTest {

    private AppSettings fastSettings() {
        AppSettings s = new AppSettings();
        s.setConnectMaxAttempts(2);
        s.setConnectBaseDelaySeconds(0.001);
        s.setConnectMaxDelaySeconds(0.005);
        return s;
    }

    static class StubProbe implements DependencyHealthProbe {
        private final String name; private final boolean up;
        StubProbe(String name, boolean up) { this.name = name; this.up = up; }
        public String name() { return name; }
        public ProbeResult probe() { return up ? ProbeResult.up(name, 1.0) : ProbeResult.down(name, 1.0, "unavailable"); }
    }

    @Test
    void passesWhenAllProbesUp() {
        StartupSmoke smoke = new StartupSmoke(List.of(new StubProbe("postgres", true), new StubProbe("redis", true)), fastSettings());
        smoke.smokeTest(); // does not throw
        assertThat(true).isTrue();
    }

    @Test
    void abortsWhenAProbeStaysDown() {
        StartupSmoke smoke = new StartupSmoke(List.of(new StubProbe("postgres", true), new StubProbe("clickhouse", false)), fastSettings());
        assertThatThrownBy(smoke::smokeTest).isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("clickhouse");
    }
}
```

- [ ] **Step 3: Run → fail**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test -Dtest=StartupSmokeTest
```
Expected: FAIL (compile — `StartupSmoke` missing).

- [ ] **Step 4: Implement StartupSmoke**

`.../core/StartupSmoke.java`. On startup, retries each probe with backoff; if any stays down after `connectMaxAttempts`, the `StartupEvent` listener throws and Micronaut aborts boot (fail-fast). `@Requires(notEnv = Environment.TEST)` keeps it out of `@MicronautTest`:
```java
package com.example.template.core;

import com.example.template.config.AppSettings;
import com.example.template.dto.health.ProbeResult;
import com.example.template.health.DependencyHealthProbe;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.Environment;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.runtime.event.annotation.EventListener;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Singleton
@Requires(notEnv = Environment.TEST)
public class StartupSmoke {

    private static final Logger LOG = LoggerFactory.getLogger(StartupSmoke.class);
    private final List<DependencyHealthProbe> probes;
    private final AppSettings settings;

    public StartupSmoke(List<DependencyHealthProbe> probes, AppSettings settings) {
        this.probes = probes;
        this.settings = settings;
    }

    @EventListener
    void onStartup(StartupEvent event) {
        smokeTest();
    }

    /** Smoke-test every dependency with backoff; throw (aborting startup) if any
     *  stays down after connectMaxAttempts. */
    public void smokeTest() {
        for (DependencyHealthProbe probe : probes) {
            try {
                Retry.connectWithBackoff(() -> {
                    ProbeResult r = probe.probe();
                    if (!"up".equals(r.status())) {
                        throw new IllegalStateException(probe.name() + " not ready: " + r.error());
                    }
                    return r;
                }, "smoke:" + probe.name(),
                   settings.getConnectMaxAttempts(),
                   settings.getConnectBaseDelaySeconds(),
                   settings.getConnectMaxDelaySeconds());
                LOG.info("startup smoke-test: {} is up", probe.name());
            } catch (Exception e) {
                throw new IllegalStateException("startup smoke-test failed for " + probe.name(), e);
            }
        }
    }
}
```

- [ ] **Step 5: Run → pass**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test -Dtest=StartupSmokeTest
```
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
cd c:/Users/Alexander/templates
git add java-webservice-template/src/main/java/com/example/template/config/AppSettings.java java-webservice-template/src/main/java/com/example/template/core/StartupSmoke.java java-webservice-template/src/test/java/com/example/template/core/StartupSmokeTest.java
git commit -m "feat(java): fail-fast startup smoke-test with backoff"
```

---

## Task 6: StreamIngestService + ingest health — TDD

**Files:**
- Create: `.../service/StreamIngestService.java`
- Test: `.../service/StreamIngestServiceTest.java`

- [ ] **Step 1: Write the failing test** (uses a fake in-memory BatchConsumer — a real object, not a mock)

`.../service/StreamIngestServiceTest.java`:
```java
package com.example.template.service;

import com.example.template.config.IngestSettings;
import com.example.template.ingestion.BatchConsumer;
import com.example.template.ingestion.ConnectionState;
import com.example.template.persistence.streamstore.LsmRow;
import com.example.template.persistence.streamstore.LsmStore;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await; // org.awaitility:awaitility test dep

class StreamIngestServiceTest {

    /** Real (non-mock) consumer driven by a queue; null sentinel ends the stream. */
    static class FakeConsumer implements BatchConsumer {
        final BlockingQueue<List<LsmRow>> queue = new LinkedBlockingQueue<>();
        volatile boolean closed = false;
        private static final List<LsmRow> SENTINEL = List.of();

        @Override
        public void consume(Consumer<List<LsmRow>> sink) throws Exception {
            while (true) {
                List<LsmRow> batch = queue.take();
                if (batch == SENTINEL) return;
                sink.accept(batch);
            }
        }
        @Override public void close() { closed = true; queue.offer(SENTINEL); }
        @Override public ConnectionState connectionState() { return closed ? ConnectionState.DOWN : ConnectionState.CONNECTED; }
    }

    private IngestSettings settings() {
        IngestSettings s = new IngestSettings();
        s.setMaxDisconnectSeconds(0); // disable watchdog/failure shutdown in test
        return s;
    }

    @Test
    void ingestedBatchesLandInTheStore() throws Exception {
        FakeConsumer consumer = new FakeConsumer();
        LsmStore store = new LsmStore();
        StreamIngestService svc = new StreamIngestService(consumer, store, settings(), () -> {});
        svc.start();

        consumer.queue.offer(List.of(new LsmRow(1, "a", "x", 0, "insert")));
        await().atMost(2, TimeUnit.SECONDS).until(() -> store.query(10).total() == 1);

        assertThat(store.query(10).rows().get(0).id()).isEqualTo(1L);
        assertThat(svc.currentHealth().connectionState()).isEqualTo("connected");
        svc.close();
    }
}
```

Add the test dependency to `pom.xml`: `<dependency><groupId>org.awaitility</groupId><artifactId>awaitility</artifactId><scope>test</scope></dependency>`.

- [ ] **Step 2: Run → fail**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test -Dtest=StreamIngestServiceTest
```
Expected: FAIL (compile — `StreamIngestService` missing).

- [ ] **Step 3: Implement StreamIngestService**

`.../service/StreamIngestService.java` (mirrors `services/stream_ingest.py`: ingest thread, backoff, consecutive-failure→SIGTERM, watchdog; implements the ingest-health seam):
```java
package com.example.template.service;

import com.example.template.config.IngestSettings;
import com.example.template.core.Timed;
import com.example.template.dto.health.IngestHealth;
import com.example.template.health.IngestHealthProvider;
import com.example.template.health.StreamIngestHealthMarker;
import com.example.template.ingestion.BatchConsumer;
import com.example.template.ingestion.ConnectionState;
import com.example.template.persistence.streamstore.LsmRow;
import com.example.template.persistence.streamstore.LsmStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class StreamIngestService implements IngestHealthProvider, StreamIngestHealthMarker, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(StreamIngestService.class);
    private static final double BASE_DELAY = 1.0, MAX_DELAY = 60.0;
    private static final int MAX_FAILURES = 5;
    private static final long JOIN_TIMEOUT_MS = 10_000;

    private final BatchConsumer consumer;
    private final LsmStore store;
    private final IngestSettings settings;
    private final Runnable shutdownHook;

    private Thread thread;
    private ScheduledExecutorService watchdog;
    private volatile Instant lastBatchAt;
    private final Instant startedAt = Instant.now();
    private final AtomicLong rowsTotal = new AtomicLong();

    public StreamIngestService(BatchConsumer consumer, LsmStore store, IngestSettings settings, Runnable shutdownHook) {
        this.consumer = consumer;
        this.store = store;
        this.settings = settings;
        this.shutdownHook = shutdownHook;
    }

    public void start() {
        thread = new Thread(this::ingestLoop, "ingest-loop");
        thread.setDaemon(true);
        thread.start();
        if (settings.getMaxDisconnectSeconds() > 0) {
            watchdog = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "ingest-watchdog"); t.setDaemon(true); return t;
            });
            long poll = Math.max(1, Math.min(settings.getMaxDisconnectSeconds() / 2, 5));
            watchdog.scheduleAtFixedRate(this::checkDisconnect, poll, poll, TimeUnit.SECONDS);
        }
    }

    private void record(List<LsmRow> batch) {
        store.ingest(batch);
        lastBatchAt = Instant.now();
        rowsTotal.addAndGet(batch.size());
    }

    private void ingestLoop() {
        int consecutiveFailures = 0;
        double delay = BASE_DELAY;
        boolean shutdownOnFailure = settings.getMaxDisconnectSeconds() > 0;
        while (true) {
            try {
                consumer.consume(batch -> {
                    String token = MDC.get("correlationId");
                    MDC.put("correlationId", UUID.randomUUID().toString());
                    try {
                        record(batch);
                    } catch (Exception e) {
                        LOG.error("ingest failed; skipping batch", e);
                    } finally {
                        if (token == null) MDC.remove("correlationId"); else MDC.put("correlationId", token);
                    }
                });
                return; // clean end — consumer closed
            } catch (Exception e) {
                consecutiveFailures++;
                LOG.error("consumer failed (failure {}/{})", consecutiveFailures, MAX_FAILURES, e);
                if (shutdownOnFailure && consecutiveFailures >= MAX_FAILURES) {
                    LOG.error("ingest: {} consecutive failures; requesting shutdown", consecutiveFailures);
                    shutdownHook.run();
                    return;
                }
                double jitter = delay * 0.25 * ThreadLocalRandom.current().nextDouble();
                try { Thread.sleep((long) ((delay + jitter) * 1000)); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); return; }
                delay = Math.min(delay * 2, MAX_DELAY);
            }
        }
    }

    private Instant disconnectedSince;

    private void checkDisconnect() {
        if (consumer.connectionState() == ConnectionState.CONNECTED) {
            disconnectedSince = null;
            return;
        }
        Instant now = Instant.now();
        if (disconnectedSince == null) {
            disconnectedSince = now;
        } else if (now.getEpochSecond() - disconnectedSince.getEpochSecond() >= settings.getMaxDisconnectSeconds()) {
            LOG.error("ingest transport not connected for {}s; requesting shutdown", settings.getMaxDisconnectSeconds());
            shutdownHook.run();
        }
    }

    /** HTTP ingest path: synchronous write with Server-Timing boundary. */
    public void ingestBatch(List<LsmRow> batch) {
        if (batch.size() > 0) {
            try (Timed t = Timed.start("ingest.lsm_write")) {
                record(batch);
            }
        }
    }

    @Override
    public IngestHealth currentHealth() {
        ConnectionState state = consumer.connectionState();
        Instant last = lastBatchAt;
        Double secondsSince = last != null ? (Instant.now().toEpochMilli() - last.toEpochMilli()) / 1000.0 : null;
        double threshold = settings.getStalenessThresholdSeconds();
        boolean stale = false;
        if (threshold > 0) {
            double elapsed = secondsSince != null ? secondsSince
                : (Instant.now().toEpochMilli() - startedAt.toEpochMilli()) / 1000.0;
            stale = elapsed > threshold;
        }
        return new IngestHealth(settings.getTransport(), state.value(),
            thread != null && thread.isAlive(), last,
            secondsSince != null ? Math.round(secondsSince * 1000.0) / 1000.0 : null,
            rowsTotal.get(), stale);
    }

    @Override
    public void close() {
        if (watchdog != null) watchdog.shutdownNow();
        consumer.close();
        if (thread != null) {
            try { thread.join(JOIN_TIMEOUT_MS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            if (thread.isAlive()) LOG.error("ingest thread did not stop within {}ms; abandoning", JOIN_TIMEOUT_MS);
        }
    }
}
```

- [ ] **Step 4: Run → pass + commit**

```bash
cd c:/Users/Alexander/templates/java-webservice-template && ./mvnw -q test -Dtest=StreamIngestServiceTest
cd c:/Users/Alexander/templates
git add java-webservice-template/pom.xml java-webservice-template/src/main/java/com/example/template/service/StreamIngestService.java java-webservice-template/src/test/java/com/example/template/service/StreamIngestServiceTest.java
git commit -m "feat(java): stream ingest service (thread, backoff, watchdog, ingest health)"
```

---

## Task 7: `/data/ingest` + `/data/cache` endpoints — TDD

**Files:**
- Modify: `.../controller/DataController.java`
- Create: `.../service/StreamIngestBeans.java` (factory wiring LsmStore + StreamIngestService as beans)
- Test: `.../controller/IngestHttpTest.java`

- [ ] **Step 1: Wire LsmStore + a no-transport StreamIngestService for the HTTP path**

For Phase-4 HTTP ingest tests we need an `LsmStore` bean and a `StreamIngestService` bean whose consumer never blocks the test. Create `.../service/StreamIngestBeans.java`:
```java
package com.example.template.service;

import com.example.template.config.IngestSettings;
import com.example.template.ingestion.BatchConsumer;
import com.example.template.persistence.streamstore.LsmStore;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;

@Factory
public class StreamIngestBeans {

    @Singleton
    LsmStore lsmStore() { return new LsmStore(); }

    /** Eager, started ingest service (the lifespan analogue). The BatchConsumer
     *  bean is selected by ConsumerFactory (Task 8); the service starts its thread here. */
    @Context
    StreamIngestService streamIngestService(BatchConsumer consumer, LsmStore store, IngestSettings settings) {
        StreamIngestService svc = new StreamIngestService(consumer, store, settings,
            () -> System.exit(3)); // SIGTERM analogue: non-zero exit so an orchestrator restarts
        svc.start();
        return svc;
    }
}
```

- [ ] **Step 2: Write the failing test**

`.../controller/IngestHttpTest.java`. POSTs Arrow IPC bytes, then reads `/data/cache`. Uses a test BatchConsumer bean that idles (so the ingest thread doesn't interfere):
```java
package com.example.template.controller;

import com.example.template.dto.CachedDataRowsResponse;
import com.example.template.ingestion.BatchConsumer;
import com.example.template.ingestion.ConnectionState;
import com.example.template.persistence.streamstore.LsmRow;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.*;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

@MicronautTest
class IngestHttpTest {

    /** Idle consumer so the background ingest thread blocks harmlessly during the HTTP test. */
    @Singleton
    @Replaces(BatchConsumer.class)
    static class IdleConsumer implements BatchConsumer {
        private final Object lock = new Object();
        private volatile boolean closed = false;
        @Override public void consume(Consumer<List<LsmRow>> sink) throws Exception {
            synchronized (lock) { while (!closed) lock.wait(); }
        }
        @Override public void close() { synchronized (lock) { closed = true; lock.notifyAll(); } }
        @Override public ConnectionState connectionState() { return ConnectionState.CONNECTED; }
    }

    @Inject @Client("/") HttpClient client;

    private byte[] arrowBatch() throws Exception {
        try (RootAllocator alloc = new RootAllocator()) {
            Schema schema = new Schema(List.of(
                new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.notNullable(new ArrowType.Utf8()), null),
                new Field("value", FieldType.notNullable(new ArrowType.Utf8()), null),
                new Field("op", FieldType.notNullable(new ArrowType.Utf8()), null)));
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc)) {
                ((BigIntVector) root.getVector("id")).setSafe(0, 7);
                ((VarCharVector) root.getVector("name")).setSafe(0, "n".getBytes(StandardCharsets.UTF_8));
                ((VarCharVector) root.getVector("value")).setSafe(0, "v".getBytes(StandardCharsets.UTF_8));
                ((VarCharVector) root.getVector("op")).setSafe(0, "insert".getBytes(StandardCharsets.UTF_8));
                root.setRowCount(1);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                try (ArrowStreamWriter w = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
                    w.start(); w.writeBatch(); w.end();
                }
                return out.toByteArray();
            }
        }
    }

    @Test
    void ingestAcceptsArrowAndCacheReturnsRowWithSeqnoAndOp() throws Exception {
        HttpStatus status = client.toBlocking().exchange(
            HttpRequest.POST("/data/ingest", arrowBatch()).contentType(MediaType.APPLICATION_OCTET_STREAM)).getStatus();
        assertThat(status).isEqualTo(HttpStatus.ACCEPTED);

        CachedDataRowsResponse r = client.toBlocking().retrieve(
            HttpRequest.GET("/data/cache?limit=10"), CachedDataRowsResponse.class);
        assertThat(r.total()).isEqualTo(1);
        assertThat(r.rows().get(0).id()).isEqualTo(7L);
        assertThat(r.rows().get(0).op()).isEqualTo("insert");
        assertThat(r.rows().get(0).seqno()).isEqualTo(0L);
    }
}
```

- [ ] **Step 2b: Run → fail**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test -Dtest=IngestHttpTest
```
Expected: FAIL (no `/data/ingest` or `/data/cache`).

- [ ] **Step 3: Add endpoints to DataController**

Add a constructor param and two methods to `.../controller/DataController.java`:
```java
// add imports:
import com.example.template.core.Timed;
import com.example.template.dto.CachedDataRow;
import com.example.template.dto.CachedDataRowsResponse;
import com.example.template.ingestion.ArrowDecoder;
import com.example.template.persistence.streamstore.LsmRow;
import com.example.template.persistence.streamstore.LsmStore;
import com.example.template.service.StreamIngestService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Post;
import java.util.List;

// add to fields + constructor:
private final LsmStore lsmStore;
private final StreamIngestService ingestService;
// constructor now: public DataController(DataService dataService, LsmStore lsmStore, StreamIngestService ingestService) { ... assign all three ... }

@Get("/cache")
public CachedDataRowsResponse getCachedData(@QueryValue(defaultValue = "10") @Min(1) @Max(100) int limit) {
    LsmStore.QueryResult result = lsmStore.query(limit);
    List<CachedDataRow> rows = result.rows().stream()
        .map(r -> new CachedDataRow(r.id(), r.name(), r.value(), r.seqno(), r.op())).toList();
    return new CachedDataRowsResponse(rows, result.total(), limit);
}

@Post(value = "/ingest", consumes = MediaType.APPLICATION_OCTET_STREAM)
public HttpResponse<?> ingest(@Body byte[] body) throws Exception {
    List<LsmRow> rows;
    try (Timed t = Timed.start("ingest.decode"); ArrowDecoder decoder = new ArrowDecoder()) {
        rows = decoder.decodeAll(body);
    }
    ingestService.ingestBatch(rows);
    return HttpResponse.accepted();
}
```

- [ ] **Step 4: Run → pass**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test -Dtest=IngestHttpTest
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd c:/Users/Alexander/templates
git add java-webservice-template/src/main/java/com/example/template/controller/DataController.java java-webservice-template/src/main/java/com/example/template/service/StreamIngestBeans.java java-webservice-template/src/test/java/com/example/template/controller/IngestHttpTest.java
git commit -m "feat(java): POST /data/ingest + GET /data/cache (seqno+op)"
```

---

## Task 8: Flight transport + embedded-server test — SPIKE-backed

**Files:**
- Create: `.../ingestion/flight/FlightBatchConsumer.java`, `.../ingestion/ConsumerFactory.java`
- Test: `.../ingestion/flight/FlightBatchConsumerTest.java` + a test Flight server

> **Spike:** Arrow Flight Java's `FlightClient`/`do_get` and embedded `FlightServer` APIs are the risk here. The test (an in-process Flight server streaming one batch → consumer → store) is the contract; adjust the consumer until green.

- [ ] **Step 1: Write the ConsumerFactory (transport selection)**

`.../ingestion/ConsumerFactory.java`:
```java
package com.example.template.ingestion;

import com.example.template.config.IngestSettings;
import com.example.template.ingestion.flight.FlightBatchConsumer;
import com.example.template.ingestion.solace.SolaceBatchConsumer;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;

@Factory
public class ConsumerFactory {
    @Singleton
    public BatchConsumer batchConsumer(IngestSettings settings) {
        return switch (settings.getTransport()) {
            case "solace" -> new SolaceBatchConsumer(settings);
            default -> new FlightBatchConsumer(settings);
        };
    }
}
```

- [ ] **Step 2: Write FlightBatchConsumer** (mirrors `ingestion/flight/client.py` lock/close semantics)

`.../ingestion/flight/FlightBatchConsumer.java`:
```java
package com.example.template.ingestion.flight;

import com.example.template.config.IngestSettings;
import com.example.template.ingestion.ArrowDecoder;
import com.example.template.ingestion.BatchConsumer;
import com.example.template.ingestion.ConnectionState;
import com.example.template.persistence.streamstore.LsmRow;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.RootAllocator;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class FlightBatchConsumer implements BatchConsumer {

    private final IngestSettings settings;
    private final RootAllocator allocator = new RootAllocator();
    private final ReentrantLock lock = new ReentrantLock();
    private volatile ConnectionState state = ConnectionState.DOWN;
    private volatile boolean closing = false;
    private FlightClient client;
    private FlightStream stream;

    public FlightBatchConsumer(IngestSettings settings) { this.settings = settings; }

    @Override
    public void consume(Consumer<List<LsmRow>> sink) throws Exception {
        Location location = Location.forGrpcInsecure(settings.getFlight().getHost(), settings.getFlight().getPort());
        lock.lock();
        try {
            if (closing) return;
            client = FlightClient.builder(allocator, location).build();
            state = ConnectionState.RECONNECTING;
        } finally { lock.unlock(); }

        FlightStream fs = client.getStream(new Ticket(settings.getFlight().getTicket().getBytes(StandardCharsets.UTF_8)));
        lock.lock();
        try {
            if (closing) { fs.close(); return; }
            stream = fs;
            state = ConnectionState.CONNECTED;
        } finally { lock.unlock(); }

        try {
            while (fs.next()) {
                sink.accept(ArrowDecoder.decodeRoot(fs.getRoot()));
            }
        } finally {
            lock.lock();
            try { if (stream == fs) stream = null; } finally { lock.unlock(); }
        }
        if (closing) return;
        state = ConnectionState.RECONNECTING;
        throw new java.io.IOException("flight stream ended unexpectedly");
    }

    @Override
    public ConnectionState connectionState() { return state; }

    @Override
    public void close() {
        FlightStream s; FlightClient c;
        lock.lock();
        try {
            closing = true; state = ConnectionState.DOWN;
            s = stream; stream = null; c = client; client = null;
        } finally { lock.unlock(); }
        try { if (s != null) s.close(); } catch (Exception ignored) {}
        try { if (c != null) c.close(); } catch (Exception ignored) {}
        allocator.close();
    }
}
```

- [ ] **Step 3: Write the embedded-server test**

`.../ingestion/flight/FlightBatchConsumerTest.java`. Stands up an in-process Flight server that streams one batch, runs the consumer manually, asserts rows decode. (Full server producer code is provided; it mirrors `tests/publishers/flight_server.py`.)
```java
package com.example.template.ingestion.flight;

import com.example.template.config.IngestSettings;
import com.example.template.persistence.streamstore.LsmRow;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.*;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

class FlightBatchConsumerTest {

    static Schema schema() {
        return new Schema(List.of(
            new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null),
            new Field("name", FieldType.notNullable(new ArrowType.Utf8()), null),
            new Field("value", FieldType.notNullable(new ArrowType.Utf8()), null),
            new Field("op", FieldType.notNullable(new ArrowType.Utf8()), null)));
    }

    static class OneBatchProducer extends NoOpFlightProducer {
        final RootAllocator allocator;
        OneBatchProducer(RootAllocator a) { this.allocator = a; }
        @Override
        public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema(), allocator)) {
                ((BigIntVector) root.getVector("id")).setSafe(0, 42);
                ((VarCharVector) root.getVector("name")).setSafe(0, "n".getBytes(StandardCharsets.UTF_8));
                ((VarCharVector) root.getVector("value")).setSafe(0, "v".getBytes(StandardCharsets.UTF_8));
                ((VarCharVector) root.getVector("op")).setSafe(0, "insert".getBytes(StandardCharsets.UTF_8));
                root.setRowCount(1);
                listener.start(root);
                listener.putNext();
                listener.completed();
            }
        }
    }

    @Test
    void consumesOneBatchFromEmbeddedFlightServer() throws Exception {
        try (RootAllocator allocator = new RootAllocator()) {
            Location location = Location.forGrpcInsecure("localhost", 0);
            try (FlightServer server = FlightServer.builder(allocator, location, new OneBatchProducer(allocator)).build()) {
                server.start();
                IngestSettings settings = new IngestSettings();
                settings.getFlight().setHost("localhost");
                settings.getFlight().setPort(server.getPort());
                settings.getFlight().setTicket("items");

                List<LsmRow> collected = new CopyOnWriteArrayList<>();
                FlightBatchConsumer consumer = new FlightBatchConsumer(settings);
                try {
                    consumer.consume(collected::addAll); // server completes the stream → consume() throws "ended unexpectedly"
                } catch (Exception expectedEnd) {
                    // clean single-batch stream end is signalled by the unexpected-end throw; rows already collected
                } finally {
                    consumer.close();
                }
                assertThat(collected).extracting(LsmRow::id).containsExactly(42L);
            }
        }
    }
}
```

- [ ] **Step 4: Run → iterate to pass**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test -Dtest=FlightBatchConsumerTest
```
Expected: PASS. If the Flight Java builder/listener method names differ in the pinned `flight-core` version, adjust the producer/consumer to match; the assertion (one row, id=42) is the contract.

- [ ] **Step 5: Commit**

```bash
cd c:/Users/Alexander/templates
git add java-webservice-template/src/main/java/com/example/template/ingestion/flight java-webservice-template/src/main/java/com/example/template/ingestion/ConsumerFactory.java java-webservice-template/src/test/java/com/example/template/ingestion/flight
git commit -m "feat(java): Arrow Flight batch consumer + transport selection"
```

---

## Task 9: Solace transport + Testcontainers test — SPIKE-backed

**Files:**
- Create: `.../ingestion/solace/SolaceBatchConsumer.java`
- Test: `.../ingestion/solace/SolaceBatchConsumerTest.java`

> **Spike:** the Solace PubSub+ Java API (`MessagingService`, `DirectMessageReceiver`) and the Testcontainers Solace broker are the risk. Mirror `ingestion/solace/client.py` structure. The test publishes an Arrow IPC message to a topic and asserts the consumer decodes it.

- [ ] **Step 1: Write SolaceBatchConsumer** (mirrors `ingestion/solace/client.py`)

`.../ingestion/solace/SolaceBatchConsumer.java`:
```java
package com.example.template.ingestion.solace;

import com.example.template.config.IngestSettings;
import com.example.template.ingestion.ArrowDecoder;
import com.example.template.ingestion.BatchConsumer;
import com.example.template.ingestion.ConnectionState;
import com.example.template.persistence.streamstore.LsmRow;
import com.solace.messaging.MessagingService;
import com.solace.messaging.config.profile.ConfigurationProfile;
import com.solace.messaging.receiver.DirectMessageReceiver;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.messaging.resources.TopicSubscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

public class SolaceBatchConsumer implements BatchConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(SolaceBatchConsumer.class);
    private static final List<LsmRow> SENTINEL = List.of();

    private final IngestSettings settings;
    private final BlockingQueue<List<LsmRow>> queue = new LinkedBlockingQueue<>();
    private volatile ConnectionState state = ConnectionState.DOWN;
    private MessagingService service;
    private DirectMessageReceiver receiver;

    public SolaceBatchConsumer(IngestSettings settings) { this.settings = settings; }

    private void connect() {
        IngestSettings.Solace s = settings.getSolace();
        Properties props = new Properties();
        props.setProperty("solace.messaging.transport.host", "tcp://" + s.getHost() + ":" + s.getPort());
        props.setProperty("solace.messaging.service.vpn-name", s.getVpn());
        props.setProperty("solace.messaging.authentication.scheme.basic.username", s.getUsername());
        props.setProperty("solace.messaging.authentication.scheme.basic.password", s.getPassword());
        service = MessagingService.builder(ConfigurationProfile.V1).fromProperties(props).build().connect();
        state = ConnectionState.CONNECTED;
    }

    @Override
    public void consume(Consumer<List<LsmRow>> sink) throws Exception {
        if (service == null) connect();
        receiver = service.createDirectMessageReceiverBuilder()
            .withSubscriptions(TopicSubscription.of(settings.getSolace().getTopic()))
            .build().start();
        receiver.receiveAsync(this::onMessage);
        while (true) {
            List<LsmRow> batch = queue.take();
            if (batch == SENTINEL) return;
            sink.accept(batch);
        }
    }

    private void onMessage(InboundMessage message) {
        try (ArrowDecoder decoder = new ArrowDecoder()) {
            queue.put(decoder.decodeAll(message.getPayloadAsBytes()));
        } catch (Exception e) {
            LOG.warn("Solace: malformed IPC message dropped", e);
        }
    }

    @Override
    public ConnectionState connectionState() { return state; }

    @Override
    public void close() {
        state = ConnectionState.DOWN;
        queue.offer(SENTINEL);
        if (receiver != null) { receiver.terminate(0); receiver = null; }
        if (service != null) { service.disconnect(); service = null; }
    }
}
```

- [ ] **Step 2: Write the Testcontainers Solace test**

`.../ingestion/solace/SolaceBatchConsumerTest.java`. Starts a Solace broker, publishes one Arrow IPC message, asserts the consumer decodes it. (Publishing uses the Solace `OutboundMessage`/`DirectMessagePublisher` API; build the Arrow bytes as in `ArrowDecoderTest`.)
```java
package com.example.template.ingestion.solace;

import com.example.template.config.IngestSettings;
import com.example.template.persistence.streamstore.LsmRow;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.solace.SolaceContainer;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
class SolaceBatchConsumerTest {

    @Container
    static final SolaceContainer SOLACE = new SolaceContainer("solace/solace-pubsub-standard:10.10");

    @Test
    void receivesAndDecodesArrowMessage() throws Exception {
        IngestSettings settings = new IngestSettings();
        IngestSettings.Solace s = settings.getSolace();
        s.setHost(SOLACE.getHost());
        s.setPort(SOLACE.getMappedPort(55555)); // SMF port; adjust to SolaceContainer's accessor
        s.setVpn(SOLACE.getVpn());
        s.setUsername(SOLACE.getUsername());
        s.setPassword(SOLACE.getPassword());
        s.setTopic("ingest/batches");

        List<LsmRow> collected = new CopyOnWriteArrayList<>();
        SolaceBatchConsumer consumer = new SolaceBatchConsumer(settings);
        Thread t = new Thread(() -> { try { consumer.consume(collected::addAll); } catch (Exception ignored) {} });
        t.setDaemon(true);
        t.start();

        // TODO in spike: publish one Arrow IPC message to "ingest/batches" using the
        // Solace DirectMessagePublisher with the bytes built as in ArrowDecoderTest.
        // Then:
        await().atMost(15, TimeUnit.SECONDS).until(() -> !collected.isEmpty());
        assertThat(collected).isNotEmpty();
        consumer.close();
    }
}
```

> The publish step is the spike's concrete deliverable — wire a `DirectMessagePublisher` and send the Arrow bytes; the `SolaceContainer` accessor names (`getVpn`/`getUsername`/SMF port) must be confirmed against the pinned Testcontainers Solace module. Done = the consumer collects ≥1 decoded row.

- [ ] **Step 3: Run → iterate to pass + commit**

```bash
cd c:/Users/Alexander/templates/java-webservice-template && ./mvnw -q test -Dtest=SolaceBatchConsumerTest
cd c:/Users/Alexander/templates
git add java-webservice-template/src/main/java/com/example/template/ingestion/solace java-webservice-template/src/test/java/com/example/template/ingestion/solace
git commit -m "feat(java): Solace batch consumer + Testcontainers test"
```

---

## Task 10: Full suite + ingest health wired into /health

- [ ] **Step 1: Confirm `DefaultIngestHealthProvider` backs off**

Because `StreamIngestService` implements `StreamIngestHealthMarker`, the Phase-2 `DefaultIngestHealthProvider` (annotated `@Requires(missingBeans = StreamIngestHealthMarker.class)`) now disables itself and `HealthService` injects the real provider. Verify by reading `/health/status` in a test that boots with the idle consumer.

- [ ] **Step 2: Run the FULL suite**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test
```
Expected: PASS — Phases 1–4. (Flight/Solace tests require Docker; the Solace broker is slow to start — allow ~30s.)

- [ ] **Step 3: Commit any test adjustments**

```bash
cd c:/Users/Alexander/templates
git add -A java-webservice-template
git commit -m "test(java): phase 4 full-suite green; real ingest health in /health"
```

---

## Phase 4 self-review checklist
- [ ] Fail-fast startup smoke-test retries each dependency probe with backoff and aborts boot if any stays down (disabled under the test env).
- [ ] `LsmStore` is append-only, single-writer, lock-free reads; `query` returns seqno-ordered rows with `seqno`+`op`.
- [ ] `POST /data/ingest` decodes Arrow IPC → 202; `GET /data/cache` returns rows carrying `seqno`+`op`.
- [ ] `StreamIngestService` runs the consume loop with jittered backoff, consecutive-failure→exit(3), and a disconnect watchdog; all disabled when `max-disconnect-seconds=0`.
- [ ] Flight consumer streams from an embedded server; Solace consumer decodes from a Testcontainers broker.
- [ ] Real ingest health appears in `/health/ready` and `/health/status` (default provider backs off).

## Deferred to Phase 5
- MCP, Docker/compose (incl. a runtime Flight server), HTTPS/443, k6 reuse, CI, JaCoCo gate, full docs.
- The `ingest.decode` / `ingest.lsm_write` boundaries already emit Server-Timing; k6 attribution validates them in Phase 5.
