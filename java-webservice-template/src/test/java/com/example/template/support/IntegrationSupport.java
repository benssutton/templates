package com.example.template.support;

import io.micronaut.test.support.TestPropertyProvider;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;

/**
 * Shared singleton containers (Postgres + ClickHouse + Redis-Stack) for integration
 * tests. Started once per JVM and reused across test classes (Testcontainers' Ryuk
 * stops them at JVM exit), so the whole suite pays one startup per dependency.
 * Mirrors the Python doctrine of exercising every test against real dependencies.
 *
 * <p>Subclasses are {@code @MicronautTest} classes. Override {@link #getProperties()}
 * and merge with {@code super.getProperties()} to add test-specific properties.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class IntegrationSupport implements TestPropertyProvider {

    protected static final PostgreSQLContainer<?> POSTGRES =
        new PostgreSQLContainer<>("postgres:16-alpine");
    protected static final ClickHouseContainer CLICKHOUSE =
        new ClickHouseContainer(DockerImageName.parse("clickhouse/clickhouse-server:24.3"));
    protected static final GenericContainer<?> REDIS =
        new GenericContainer<>(DockerImageName.parse("redis/redis-stack-server:latest")).withExposedPorts(6379);

    static {
        POSTGRES.start();
        CLICKHOUSE.start();
        REDIS.start();
    }

    @Override
    public Map<String, String> getProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("datasources.default.url", POSTGRES.getJdbcUrl());
        props.put("datasources.default.username", POSTGRES.getUsername());
        props.put("datasources.default.password", POSTGRES.getPassword());
        props.put("datasources.clickhouse.url",
            "jdbc:ch://" + CLICKHOUSE.getHost() + ":" + CLICKHOUSE.getMappedPort(8123) + "/default");
        props.put("datasources.clickhouse.username", CLICKHOUSE.getUsername());
        props.put("datasources.clickhouse.password", CLICKHOUSE.getPassword());
        props.put("redis.uri", "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379) + "/0");
        return props;
    }
}
