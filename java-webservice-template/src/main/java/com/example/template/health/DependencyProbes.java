package com.example.template.health;

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.data.connection.jdbc.advice.DelegatingDataSource;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.sql.Connection;
import java.sql.Statement;
import javax.sql.DataSource;
import redis.clients.jedis.RedisClient;

/**
 * Registers one {@link PingProbe} per store; HealthService and StartupSmoke receive them as {@code
 * List<DependencyHealthProbe>}. Adding a store means adding a method here.
 */
@Factory
public class DependencyProbes {

    @Singleton
    @Named("postgres")
    @Requires(beans = DataSource.class)
    DependencyHealthProbe postgres(DataSource dataSource) {
        return new PingProbe("postgres", () -> selectOne(dataSource));
    }

    @Singleton
    @Named("clickhouse")
    @Requires(beans = DataSource.class)
    DependencyHealthProbe clickhouse(@Named("clickhouse") DataSource dataSource) {
        return new PingProbe("clickhouse", () -> selectOne(dataSource));
    }

    @Singleton
    @Named("redis")
    DependencyHealthProbe redis(RedisClient redis) {
        return new PingProbe("redis", redis::ping);
    }

    /**
     * Unwraps Micronaut Data's transaction-aware proxy, whose connections need an active
     * {@code @Transactional} scope, to borrow a real pooled connection.
     */
    private static void selectOne(DataSource dataSource) throws Exception {
        DataSource raw = DelegatingDataSource.unwrapDataSource(dataSource);
        try (Connection c = raw.getConnection();
                Statement s = c.createStatement()) {
            s.execute("SELECT 1");
        }
    }
}
