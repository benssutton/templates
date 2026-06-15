package com.example.template.support;

import io.micronaut.test.support.TestPropertyProvider;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.PostgreSQLContainer;

import java.util.Map;

/**
 * Shared singleton Postgres container for integration tests. Started once per JVM
 * and reused across test classes (Testcontainers' Ryuk stops it at JVM exit), so
 * the whole suite pays a single container startup. Mirrors the Python doctrine of
 * exercising every test against a real database rather than mocks.
 *
 * <p>Subclasses are {@code @MicronautTest} classes. Override {@link #getProperties()}
 * and merge with {@code super.getProperties()} if a test needs extra properties.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class PostgresTestSupport implements TestPropertyProvider {

    protected static final PostgreSQLContainer<?> POSTGRES =
        new PostgreSQLContainer<>("postgres:16-alpine");

    static {
        POSTGRES.start();
    }

    @Override
    public Map<String, String> getProperties() {
        return Map.of(
            "datasources.default.url", POSTGRES.getJdbcUrl(),
            "datasources.default.username", POSTGRES.getUsername(),
            "datasources.default.password", POSTGRES.getPassword());
    }
}
