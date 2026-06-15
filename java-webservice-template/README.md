# java-webservice-template

A Micronaut (Java 25) mirror of `python-webservice-template`, built so the two
codebases map onto each other. See
`../docs/superpowers/specs/2026-06-15-java-webservice-template-design.md` for the
design and the Python↔Java mapping, and `../docs/superpowers/plans/` for the
phased implementation plans.

**Status:** Phase 1 complete — foundation + `/config` vertical slice (Postgres
via Micronaut Data, schema-on-startup, virtual-thread controller, end-to-end
Testcontainers test). Phases 2–5 (observability, ClickHouse/Redis stores, stream
ingestion + LSM, MCP/Docker/CI) follow.

## Stack
- Micronaut 5 on Java 25 (Temurin), Maven
- Micronaut Data JDBC + HikariCP (Postgres), Micronaut Serialization, Jakarta Validation
- JUnit 5 + Micronaut Test + Testcontainers (real dependencies, no mocks)

## Build & test
```bash
cd java-webservice-template
# JAVA_HOME must point at a JDK 25; Docker must be running (Testcontainers).
./mvnw test
```

On Windows + Docker Desktop, the `windows-docker` Maven profile auto-activates to
pin `DOCKER_HOST` to the engine named pipe; on Linux/CI it stays inactive.
