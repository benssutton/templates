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
