# Java Webservice Template — Phase 1: Foundation + Config Vertical Slice

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the `java-webservice-template` Micronaut project (Java 25, Maven) with a complete vertical slice — a Postgres-backed `/config` endpoint — proving the framework, DI, Micronaut Data, schema bootstrap, and the Testcontainers test pattern that every later phase reuses.

**Architecture:** A single-module Micronaut 4.x app on Java 25. Controllers run blocking handlers on virtual threads (`@ExecuteOn(TaskExecutors.BLOCKING)`). Persistence uses Micronaut Data JDBC repositories over a HikariCP pool. Schema is created at startup by executing the same `db/postgres-init.sql` resource that docker-compose will mount (single source of truth, mirroring the Python lifespan). Native DI (`@Singleton` + constructor injection) replaces the Python hand-rolled container; `@MicronautTest` provides the per-test isolated context that mirrors `create_app`.

**Tech Stack:** Micronaut 4.8.x, Java 25, Maven, Micronaut Data JDBC, HikariCP, PostgreSQL driver, Micronaut Serialization (Jackson), Jakarta Validation, JUnit 5, Micronaut Test, Testcontainers (postgres), AssertJ.

**Reference:** Mirrors `python-webservice-template/{routers,services,schemas,persistence/transaction_store}` for the config feature. Spec: `docs/superpowers/specs/2026-06-15-java-webservice-template-design.md`.

**Conventions for every task below:**
- All paths are relative to the repo root `c:/Users/Alexander/templates/`.
- The new module lives at `java-webservice-template/`.
- Run Maven from inside `java-webservice-template/` (`cd java-webservice-template`).
- Java package root is `com.example.template`; source under `src/main/java/com/example/template/`, tests under `src/test/java/com/example/template/`, resources under `src/main/resources/`.

---

## File structure produced by this phase

| File | Responsibility |
|---|---|
| `java-webservice-template/pom.xml` | Maven build: Micronaut parent, Java 25, dependencies |
| `src/main/java/com/example/template/Application.java` | Micronaut entry point (the `create_app`/`main.py` analogue) |
| `src/main/resources/application.yml` | Datasource + app config (the `settings.py` analogue, config subset) |
| `src/main/resources/logback.xml` | Logging config |
| `src/main/resources/db/postgres-init.sql` | Config table DDL (single source of truth, shared with compose later) |
| `src/main/java/.../dto/ConfigSetRequest.java` | Request record (mirrors `schemas/config.py`) |
| `src/main/java/.../dto/ConfigEntry.java` | Response record |
| `src/main/java/.../persistence/transactionstore/postgres/ConfigurationEntity.java` | Micronaut Data entity for `configuration` table |
| `src/main/java/.../persistence/transactionstore/postgres/ConfigurationRepository.java` | JDBC repository (upsert + ordered list) |
| `src/main/java/.../persistence/SchemaInitializer.java` | Runs `postgres-init.sql` on startup (mirrors lifespan DDL) |
| `src/main/java/.../service/ConfigService.java` | Config business logic (mirrors `services/config.py`) |
| `src/main/java/.../controller/ConfigController.java` | `/config` REST endpoints (mirrors `routers/config.py`) |
| `src/test/java/.../controller/ConfigControllerTest.java` | End-to-end HTTP test against a real Postgres container |

---

## Task 1: Bootstrap the Micronaut Maven project

**Files:**
- Create: `java-webservice-template/` (whole project tree, via Micronaut Launch)

- [ ] **Step 1: Generate the project from Micronaut Launch**

Run from the repo root (`c:/Users/Alexander/templates/`). This downloads a reproducible starter zip with exactly the features Phase 1 needs and unzips it into `java-webservice-template/`:

```bash
cd c:/Users/Alexander/templates
curl -L "https://launch.micronaut.io/create/default/com.example.template.java-webservice-template?lang=JAVA&build=MAVEN&test=JUNIT&javaVersion=JDK_21&features=data-jdbc&features=postgres&features=serialization-jackson&features=validation&features=test-resources" -o mn.zip
unzip -q mn.zip -d mn-tmp
# The zip's top-level dir is the artifactId; move its contents to java-webservice-template/
mkdir -p java-webservice-template
cp -r mn-tmp/java-webservice-template/* java-webservice-template/ 2>/dev/null || cp -r mn-tmp/*/. java-webservice-template/
rm -rf mn.zip mn-tmp
ls java-webservice-template
```

Expected: `java-webservice-template/` now contains `pom.xml`, `src/`, `mvnw`, `mvnw.cmd`, `.mvn/`, etc. (If Launch's javaVersion list lacks `JDK_21`, use `JDK_17`; the next task pins the real version to 25.)

- [ ] **Step 2: Remove the generated placeholder Application test if present**

Micronaut Launch sometimes generates a trivial context-start test. Delete it so our own tests define the suite:

```bash
cd c:/Users/Alexander/templates/java-webservice-template
rm -f src/test/java/com/example/template/java_webservice_template/*.java
find src/test -name "*Test.java" -maxdepth 6 -print
```

Expected: no leftover generated `*Test.java` (empty output is fine).

- [ ] **Step 3: Move generated Application.java to the package root**

Launch nests classes under a sub-package derived from the artifact id (`com.example.template.java_webservice_template`). Flatten to `com.example.template`:

```bash
cd c:/Users/Alexander/templates/java-webservice-template
mkdir -p src/main/java/com/example/template
# move any generated Application.java up, then drop the nested package dir
find src/main/java -name 'Application.java' -exec sh -c 'cat "$1"' _ {} \;
rm -rf src/main/java/com/example/template/java_webservice_template
ls src/main/java/com/example/template
```

We will author `Application.java` explicitly in Task 3, so removing the nested copy is fine.

- [ ] **Step 4: Commit the bootstrap**

```bash
cd c:/Users/Alexander/templates
git add java-webservice-template
git commit -m "chore(java): bootstrap Micronaut Maven project for java-webservice-template"
```

---

## Task 2: Pin Java 25 and lock the dependency set

**Files:**
- Modify: `java-webservice-template/pom.xml`

- [ ] **Step 1: Set the Java version properties to 25**

In `java-webservice-template/pom.xml`, inside `<properties>`, set the JDK/release properties to 25 (replace whatever Launch generated):

```xml
<properties>
  <packaging>jar</packaging>
  <jdk.version>25</jdk.version>
  <release.version>25</release.version>
  <maven.compiler.release>25</maven.compiler.release>
  <micronaut.version>4.8.2</micronaut.version>
  <micronaut.runtime>netty</micronaut.runtime>
  <micronaut.aot.enabled>false</micronaut.aot.enabled>
  <exec.mainClass>com.example.template.Application</exec.mainClass>
  <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
</properties>
```

(If Micronaut 4.8.2 is not the latest 4.x at build time, use the latest 4.x — the parent POM version below must match.)

- [ ] **Step 2: Confirm the required dependencies are present**

Ensure `java-webservice-template/pom.xml` `<dependencies>` contains exactly these (Launch should have added most; add any missing). Annotation processors go in `<annotationProcessorPaths>` which the Micronaut parent manages via the listed `*-processor`/`*-parameters` deps:

```xml
<!-- HTTP + DI core (from micronaut parent BOM, no version) -->
<dependency><groupId>io.micronaut</groupId><artifactId>micronaut-http-server-netty</artifactId><scope>compile</scope></dependency>
<dependency><groupId>io.micronaut</groupId><artifactId>micronaut-inject</artifactId><scope>compile</scope></dependency>
<dependency><groupId>io.micronaut.serde</groupId><artifactId>micronaut-serde-jackson</artifactId><scope>compile</scope></dependency>
<dependency><groupId>io.micronaut.validation</groupId><artifactId>micronaut-validation</artifactId><scope>compile</scope></dependency>

<!-- Data JDBC + Postgres -->
<dependency><groupId>io.micronaut.data</groupId><artifactId>micronaut-data-jdbc</artifactId><scope>compile</scope></dependency>
<dependency><groupId>io.micronaut.sql</groupId><artifactId>micronaut-jdbc-hikari</artifactId><scope>compile</scope></dependency>
<dependency><groupId>org.postgresql</groupId><artifactId>postgresql</artifactId><scope>runtime</scope></dependency>

<!-- Logging -->
<dependency><groupId>ch.qos.logback</groupId><artifactId>logback-classic</artifactId><scope>runtime</scope></dependency>

<!-- Test -->
<dependency><groupId>io.micronaut.test</groupId><artifactId>micronaut-test-junit5</artifactId><scope>test</scope></dependency>
<dependency><groupId>org.junit.jupiter</groupId><artifactId>junit-jupiter-api</artifactId><scope>test</scope></dependency>
<dependency><groupId>org.junit.jupiter</groupId><artifactId>junit-jupiter-engine</artifactId><scope>test</scope></dependency>
<dependency><groupId>org.testcontainers</groupId><artifactId>postgresql</artifactId><scope>test</scope></dependency>
<dependency><groupId>org.testcontainers</groupId><artifactId>junit-jupiter</artifactId><scope>test</scope></dependency>
<dependency><groupId>org.assertj</groupId><artifactId>assertj-core</artifactId><scope>test</scope></dependency>
```

If Launch added `micronaut-test-resources`/`test-resources` BOM entries, remove them — this template uses explicit Testcontainers, not Test Resources (transparency: the container lifecycle is visible in the test).

- [ ] **Step 3: Verify the build resolves and compiles**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q -DskipTests compile
```

Expected: `BUILD SUCCESS`. (First run downloads dependencies; allow a few minutes.)

- [ ] **Step 4: Commit**

```bash
cd c:/Users/Alexander/templates
git add java-webservice-template/pom.xml
git commit -m "build(java): pin Java 25 and lock Phase 1 dependency set"
```

---

## Task 3: Application entry point + base config + logging

**Files:**
- Create: `java-webservice-template/src/main/java/com/example/template/Application.java`
- Create: `java-webservice-template/src/main/resources/application.yml`
- Create: `java-webservice-template/src/main/resources/logback.xml`

- [ ] **Step 1: Write the Application entry point**

`src/main/java/com/example/template/Application.java`:

```java
package com.example.template;

import io.micronaut.runtime.Micronaut;

/**
 * Application entry point.
 *
 * <p>Maps to the Python template's {@code main.py}. Where Python hand-rolls a DI
 * container in {@code core/container.py} and builds an isolated app via
 * {@code create_app(settings)}, here Micronaut's {@code ApplicationContext} is
 * the container: beans are discovered by {@code @Singleton} + constructor
 * injection, and tests get an isolated context per class via {@code @MicronautTest}
 * (the {@code create_app} isolation analogue).
 */
public class Application {
    public static void main(String[] args) {
        Micronaut.run(Application.class, args);
    }
}
```

- [ ] **Step 2: Write application.yml (config subset for Phase 1)**

`src/main/resources/application.yml`. Datasource defaults mirror the Python `postgres_url` default; env vars override (the `BaseSettings` analogue). `schema-generate: NONE` because `SchemaInitializer` owns DDL:

```yaml
micronaut:
  application:
    name: java-template
  server:
    port: ${SERVER_PORT:8080}

datasources:
  default:
    url: ${POSTGRES_URL:`jdbc:postgresql://localhost:5432/appdb`}
    username: ${POSTGRES_USER:appuser}
    password: ${POSTGRES_PASSWORD:password}
    driver-class-name: org.postgresql.Driver
    dialect: POSTGRES
    schema-generate: NONE
    minimum-idle: 2
    maximum-pool-size: 10
```

Note: HTTPS-on-443 (the Python parity target) is configured in the Docker phase (Phase 5). Tests and local dev run plain HTTP on 8080, exactly as the Python tests exercise the ASGI app without real TLS.

Note on virtual threads: on Java 21+, Micronaut's `BLOCKING` executor is backed by a virtual-thread-per-task executor, so `@ExecuteOn(TaskExecutors.BLOCKING)` (used on controllers) runs handlers on virtual threads with no extra config.

- [ ] **Step 3: Write logback.xml**

`src/main/resources/logback.xml` (correlation-id MDC pattern is wired in Phase 2; for now a clean console pattern):

```xml
<configuration>
  <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
    <encoder>
      <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} %-5level [%thread] %logger{36} - %msg%n</pattern>
    </encoder>
  </appender>
  <root level="INFO">
    <appender-ref ref="STDOUT"/>
  </root>
</configuration>
```

- [ ] **Step 4: Verify it still compiles**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q -DskipTests compile
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 5: Commit**

```bash
cd c:/Users/Alexander/templates
git add java-webservice-template/src/main/java/com/example/template/Application.java java-webservice-template/src/main/resources/application.yml java-webservice-template/src/main/resources/logback.xml
git commit -m "feat(java): application entry point, base config, logging"
```

---

## Task 4: Config DTOs

**Files:**
- Create: `java-webservice-template/src/main/java/com/example/template/dto/ConfigSetRequest.java`
- Create: `java-webservice-template/src/main/java/com/example/template/dto/ConfigEntry.java`
- Test: `java-webservice-template/src/test/java/com/example/template/dto/ConfigDtoSerdeTest.java`

- [ ] **Step 1: Write the failing serialization test**

`src/test/java/com/example/template/dto/ConfigDtoSerdeTest.java`. Proves the records are `@Serdeable` (round-trip through Micronaut's JSON mapper) — mirrors the Pydantic model contract:

```java
package com.example.template.dto;

import io.micronaut.json.JsonMapper;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@MicronautTest(startApplication = false)
class ConfigDtoSerdeTest {

    @Inject
    JsonMapper json;

    @Test
    void configEntryRoundTrips() throws Exception {
        ConfigEntry entry = new ConfigEntry("featureX", "on");
        String encoded = new String(json.writeValueAsBytes(entry));
        assertThat(encoded).contains("\"key\":\"featureX\"").contains("\"value\":\"on\"");
        ConfigEntry decoded = json.readValue(encoded, ConfigEntry.class);
        assertThat(decoded).isEqualTo(entry);
    }

    @Test
    void configSetRequestDecodes() throws Exception {
        ConfigSetRequest req = json.readValue("{\"key\":\"k\",\"value\":\"v\"}", ConfigSetRequest.class);
        assertThat(req.key()).isEqualTo("k");
        assertThat(req.value()).isEqualTo("v");
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test -Dtest=ConfigDtoSerdeTest
```

Expected: FAIL — compilation error, `ConfigEntry`/`ConfigSetRequest` do not exist.

- [ ] **Step 3: Write the DTOs**

`src/main/java/com/example/template/dto/ConfigSetRequest.java` (mirrors `schemas/config.py::ConfigSetRequest`):

```java
package com.example.template.dto;

import io.micronaut.serde.annotation.Serdeable;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

@Serdeable
public record ConfigSetRequest(@NotBlank String key, @NotNull String value) {}
```

`src/main/java/com/example/template/dto/ConfigEntry.java` (mirrors `schemas/config.py::ConfigEntry`):

```java
package com.example.template.dto;

import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record ConfigEntry(String key, String value) {}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test -Dtest=ConfigDtoSerdeTest
```

Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
cd c:/Users/Alexander/templates
git add java-webservice-template/src/main/java/com/example/template/dto java-webservice-template/src/test/java/com/example/template/dto
git commit -m "feat(java): config request/response DTOs (records)"
```

---

## Task 5: Postgres schema DDL + entity + repository + schema initializer

**Files:**
- Create: `java-webservice-template/src/main/resources/db/postgres-init.sql`
- Create: `java-webservice-template/src/main/java/com/example/template/persistence/transactionstore/postgres/ConfigurationEntity.java`
- Create: `java-webservice-template/src/main/java/com/example/template/persistence/transactionstore/postgres/ConfigurationRepository.java`
- Create: `java-webservice-template/src/main/java/com/example/template/persistence/SchemaInitializer.java`

- [ ] **Step 1: Write the DDL resource**

`src/main/resources/db/postgres-init.sql` (identical to `python-webservice-template/scripts/postgres-init.sql` — the single source of truth):

```sql
CREATE TABLE IF NOT EXISTS configuration (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
```

- [ ] **Step 2: Write the entity**

`.../persistence/transactionstore/postgres/ConfigurationEntity.java`. `@Id` on the assigned (non-generated) `key`, mirroring the Python TEXT PRIMARY KEY:

```java
package com.example.template.persistence.transactionstore.postgres;

import io.micronaut.data.annotation.Id;
import io.micronaut.data.annotation.MappedEntity;
import io.micronaut.serde.annotation.Serdeable;

@Serdeable
@MappedEntity("configuration")
public record ConfigurationEntity(@Id String key, String value) {}
```

- [ ] **Step 3: Write the repository**

`.../persistence/transactionstore/postgres/ConfigurationRepository.java`. The upsert mirrors the Python `INSERT ... ON CONFLICT DO UPDATE`; the ordered read mirrors `SELECT ... ORDER BY key`:

```java
package com.example.template.persistence.transactionstore.postgres;

import io.micronaut.data.annotation.Query;
import io.micronaut.data.jdbc.annotation.JdbcRepository;
import io.micronaut.data.model.query.builder.sql.Dialect;
import io.micronaut.data.repository.CrudRepository;

import java.util.List;

@JdbcRepository(dialect = Dialect.POSTGRES)
public interface ConfigurationRepository extends CrudRepository<ConfigurationEntity, String> {

    @Query("INSERT INTO configuration(key, value) VALUES (:key, :value) "
         + "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")
    void upsert(String key, String value);

    @Query("SELECT key, value FROM configuration ORDER BY key")
    List<ConfigurationEntity> findAllOrdered();
}
```

- [ ] **Step 4: Write the schema initializer**

`.../persistence/SchemaInitializer.java`. Runs the DDL on startup, mirroring the Python lifespan executing `postgres-init.sql`. Idempotent (`CREATE TABLE IF NOT EXISTS`):

```java
package com.example.template.persistence;

import io.micronaut.context.event.StartupEvent;
import io.micronaut.runtime.event.annotation.EventListener;
import jakarta.inject.Singleton;

import javax.sql.DataSource;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;

/**
 * Creates the database schema at startup by executing {@code db/postgres-init.sql}.
 * Mirrors the Python lifespan running the same DDL file, keeping the SQL as the
 * single source of truth shared with docker-compose.
 */
@Singleton
public class SchemaInitializer {

    private final DataSource dataSource;

    public SchemaInitializer(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @EventListener
    void onStartup(StartupEvent event) throws Exception {
        String sql;
        try (InputStream in = getClass().getResourceAsStream("/db/postgres-init.sql")) {
            if (in == null) {
                throw new IllegalStateException("db/postgres-init.sql not found on classpath");
            }
            sql = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }
}
```

- [ ] **Step 5: Verify it compiles (Micronaut Data annotation processor validates the repository)**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q -DskipTests compile
```

Expected: `BUILD SUCCESS` — the Data processor accepts the entity and both `@Query` methods.

- [ ] **Step 6: Commit**

```bash
cd c:/Users/Alexander/templates
git add java-webservice-template/src/main/resources/db java-webservice-template/src/main/java/com/example/template/persistence
git commit -m "feat(java): postgres configuration entity, repository, schema initializer"
```

---

## Task 6: Config service

**Files:**
- Create: `java-webservice-template/src/main/java/com/example/template/service/ConfigService.java`

- [ ] **Step 1: Write the service**

`.../service/ConfigService.java` (mirrors `services/config.py`; the `timed(...)` boundary instrumentation is added in Phase 2 with the observability core):

```java
package com.example.template.service;

import com.example.template.dto.ConfigEntry;
import com.example.template.persistence.transactionstore.postgres.ConfigurationRepository;
import jakarta.inject.Singleton;

import java.util.List;

@Singleton
public class ConfigService {

    private final ConfigurationRepository repository;

    public ConfigService(ConfigurationRepository repository) {
        this.repository = repository;
    }

    public List<ConfigEntry> getAll() {
        return repository.findAllOrdered().stream()
            .map(e -> new ConfigEntry(e.key(), e.value()))
            .toList();
    }

    public ConfigEntry set(String key, String value) {
        repository.upsert(key, value);
        return new ConfigEntry(key, value);
    }
}
```

- [ ] **Step 2: Verify it compiles**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q -DskipTests compile
```

Expected: `BUILD SUCCESS`.

- [ ] **Step 3: Commit**

```bash
cd c:/Users/Alexander/templates
git add java-webservice-template/src/main/java/com/example/template/service/ConfigService.java
git commit -m "feat(java): config service"
```

---

## Task 7: Config controller + end-to-end Testcontainers test

**Files:**
- Create: `java-webservice-template/src/main/java/com/example/template/controller/ConfigController.java`
- Test: `java-webservice-template/src/test/java/com/example/template/controller/ConfigControllerTest.java`

- [ ] **Step 1: Write the failing end-to-end test**

`.../controller/ConfigControllerTest.java`. Starts a real Postgres via Testcontainers, injects its JDBC coordinates with `TestPropertyProvider`, and drives the live HTTP server — the JUnit/Testcontainers analogue of the Python HTTPX + testcontainers config test:

```java
package com.example.template.controller;

import com.example.template.dto.ConfigEntry;
import com.example.template.dto.ConfigSetRequest;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
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

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@MicronautTest
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConfigControllerTest implements TestPropertyProvider {

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
        new PostgreSQLContainer<>("postgres:16-alpine");

    @Override
    public Map<String, String> getProperties() {
        if (!POSTGRES.isRunning()) {
            POSTGRES.start();
        }
        return Map.of(
            "datasources.default.url", POSTGRES.getJdbcUrl(),
            "datasources.default.username", POSTGRES.getUsername(),
            "datasources.default.password", POSTGRES.getPassword()
        );
    }

    @Inject
    @Client("/")
    HttpClient client;

    @Test
    void postReturns201AndGetListsTheEntry() {
        HttpResponse<ConfigEntry> created = client.toBlocking().exchange(
            HttpRequest.POST("/config", new ConfigSetRequest("featureX", "on")),
            ConfigEntry.class);
        assertThat(created.getStatus()).isEqualTo(HttpStatus.CREATED);
        assertThat(created.body()).isEqualTo(new ConfigEntry("featureX", "on"));

        List<ConfigEntry> all = client.toBlocking().retrieve(
            HttpRequest.GET("/config"),
            Argument.listOf(ConfigEntry.class));
        assertThat(all).contains(new ConfigEntry("featureX", "on"));
    }

    @Test
    void postIsUpsert() {
        client.toBlocking().exchange(HttpRequest.POST("/config", new ConfigSetRequest("k", "v1")));
        client.toBlocking().exchange(HttpRequest.POST("/config", new ConfigSetRequest("k", "v2")));

        List<ConfigEntry> all = client.toBlocking().retrieve(
            HttpRequest.GET("/config"),
            Argument.listOf(ConfigEntry.class));
        assertThat(all.stream().filter(e -> e.key().equals("k")).map(ConfigEntry::value).toList())
            .containsExactly("v2");
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test -Dtest=ConfigControllerTest
```

Expected: FAIL — no controller mapped at `/config`, POST returns 404 (and/or compile error referencing `ConfigController` is not needed since the test doesn't import it; the failure is the 404 assertion). Requires Docker running for Testcontainers.

- [ ] **Step 3: Write the controller**

`.../controller/ConfigController.java` (mirrors `routers/config.py`: POST→201, GET→list). `@ExecuteOn(TaskExecutors.BLOCKING)` runs the blocking JDBC work on a virtual thread:

```java
package com.example.template.controller;

import com.example.template.dto.ConfigEntry;
import com.example.template.dto.ConfigSetRequest;
import com.example.template.service.ConfigService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import jakarta.validation.Valid;

import java.util.List;

@Controller("/config")
@ExecuteOn(TaskExecutors.BLOCKING)
public class ConfigController {

    private final ConfigService configService;

    public ConfigController(ConfigService configService) {
        this.configService = configService;
    }

    @Post
    public HttpResponse<ConfigEntry> set(@Valid @Body ConfigSetRequest body) {
        return HttpResponse.created(configService.set(body.key(), body.value()));
    }

    @Get
    public List<ConfigEntry> getAll() {
        return configService.getAll();
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test -Dtest=ConfigControllerTest
```

Expected: PASS (2 tests). A real Postgres container starts, `SchemaInitializer` creates the `configuration` table, and both round-trips succeed.

- [ ] **Step 5: Run the full test suite**

```bash
cd c:/Users/Alexander/templates/java-webservice-template
./mvnw -q test
```

Expected: PASS — `ConfigDtoSerdeTest` (2) + `ConfigControllerTest` (2).

- [ ] **Step 6: Commit**

```bash
cd c:/Users/Alexander/templates
git add java-webservice-template/src/main/java/com/example/template/controller java-webservice-template/src/test/java/com/example/template/controller
git commit -m "feat(java): config controller with end-to-end Testcontainers test"
```

---

## Task 8: Phase-1 README stub and .gitignore

**Files:**
- Create: `java-webservice-template/README.md`
- Modify/Create: `java-webservice-template/.gitignore`

- [ ] **Step 1: Confirm a Java .gitignore exists and covers build output**

Launch generates a `.gitignore`. Ensure it ignores Maven/IDE output; append if missing:

```bash
cd c:/Users/Alexander/templates/java-webservice-template
cat .gitignore 2>/dev/null | grep -q "target/" || printf "\ntarget/\n.idea/\n*.iml\n" >> .gitignore
grep -E "target/" .gitignore
```

Expected: `target/` present.

- [ ] **Step 2: Write a short README stub**

`java-webservice-template/README.md` (expanded fully in Phase 5; for now a navigational stub so the directory is self-describing):

```markdown
# java-webservice-template

A Micronaut (Java 25) mirror of `python-webservice-template`, built so the two
codebases map onto each other. See `../docs/superpowers/specs/2026-06-15-java-webservice-template-design.md`
for the design and the Python↔Java mapping.

**Status:** Phase 1 — foundation + `/config` vertical slice (Postgres via Micronaut Data).

## Run the tests
```bash
cd java-webservice-template
./mvnw test   # requires Docker (Testcontainers)
```
```

- [ ] **Step 3: Commit**

```bash
cd c:/Users/Alexander/templates
git add java-webservice-template/.gitignore java-webservice-template/README.md
git commit -m "docs(java): phase-1 README stub and gitignore"
```

---

## Phase 1 self-review checklist (for the implementer)

Before declaring Phase 1 done, confirm:
- [ ] `./mvnw test` is green with Docker running (4 tests across 2 classes).
- [ ] `./mvnw -DskipTests package` produces a runnable jar under `target/`.
- [ ] No Micronaut Test Resources dependency remains (explicit Testcontainers only).
- [ ] Package layout is `com.example.template.{dto,persistence.transactionstore.postgres,persistence,service,controller}` — matching the spec's mapping table.
- [ ] `db/postgres-init.sql` is byte-identical to the Python `scripts/postgres-init.sql`.

## What Phase 1 deliberately defers (handled in later phases)
- `timed(...)` Server-Timing boundaries around the config queries → Phase 2.
- `ConfigService.healthCheck()` probe (the `_probe`/readiness wiring) → Phase 2.
- HTTPS/443, keystore generation → Phase 5 (Docker).
- The `/config` trailing-slash exact parity and OpenAPI/Swagger UI → Phase 2/5.
```
