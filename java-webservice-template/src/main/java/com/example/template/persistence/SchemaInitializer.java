package com.example.template.persistence;

import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.data.connection.jdbc.advice.DelegatingDataSource;
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
// Only wire schema setup when a DataSource is actually present. Context-only tests
// (and any deployment running without a configured datasource) disable the datasource,
// in which case there is no schema to create and this listener must not be required.
@Requires(beans = DataSource.class)
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
        // When micronaut-data-jdbc is on the classpath the injected DataSource is a
        // transaction-aware ContextualConnection proxy whose connections require an
        // active @Connectable/@Transactional scope. Schema setup runs outside any such
        // scope, so unwrap to the underlying pool DataSource to obtain a real JDBC
        // connection for the one-off DDL execution.
        DataSource rawDataSource = DelegatingDataSource.unwrapDataSource(dataSource);
        try (Connection conn = rawDataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }
}
