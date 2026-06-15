package com.example.template.service;

import com.example.template.dto.health.ProbeResult;
import com.example.template.health.DependencyHealthProbe;
import io.micronaut.context.annotation.Requires;
import io.micronaut.data.connection.jdbc.advice.DelegatingDataSource;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;

/** ClickHouse dependency probe (mirrors Python DataService.health_check). */
@Singleton
@Requires(beans = DataSource.class)
public class DataHealthProbe implements DependencyHealthProbe {

    private static final Logger LOG = LoggerFactory.getLogger(DataHealthProbe.class);
    private final DataSource dataSource;

    public DataHealthProbe(@Named("clickhouse") DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public String name() {
        return "clickhouse";
    }

    @Override
    public ProbeResult probe() {
        long start = System.nanoTime();
        DataSource raw = DelegatingDataSource.unwrapDataSource(dataSource);
        try (Connection c = raw.getConnection(); Statement s = c.createStatement()) {
            s.execute("SELECT 1");
            return ProbeResult.up("clickhouse", ms(start));
        } catch (Exception e) {
            LOG.error("clickhouse health check failed: {}", e.toString());
            return ProbeResult.down("clickhouse", ms(start), "unavailable");
        }
    }

    private static double ms(long startNanos) {
        return Math.round((System.nanoTime() - startNanos) / 1_000_000.0 * 100.0) / 100.0;
    }
}
