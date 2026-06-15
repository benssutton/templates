package com.example.template.service;

import com.example.template.dto.health.ProbeResult;
import com.example.template.health.DependencyHealthProbe;
import io.micronaut.context.annotation.Requires;
import io.micronaut.data.connection.jdbc.advice.DelegatingDataSource;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;

/** Postgres dependency probe (mirrors Python ConfigService.health_check; runs SELECT 1). */
@Singleton
@Requires(beans = DataSource.class)
public class ConfigHealthProbe implements DependencyHealthProbe {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigHealthProbe.class);
    private final DataSource dataSource;

    public ConfigHealthProbe(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public String name() {
        return "postgres";
    }

    @Override
    public ProbeResult probe() {
        long start = System.nanoTime();
        // Unwrap the transaction-aware proxy to get a real pooled connection outside
        // any @Transactional scope (same reason as SchemaInitializer).
        DataSource raw = DelegatingDataSource.unwrapDataSource(dataSource);
        try (Connection c = raw.getConnection(); Statement s = c.createStatement()) {
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
