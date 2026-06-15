package com.example.template.persistence.analyticsstore.clickhouse;

import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.data.connection.jdbc.advice.DelegatingDataSource;
import io.micronaut.runtime.event.annotation.EventListener;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

import javax.sql.DataSource;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;

/** Creates the ClickHouse schema at startup from {@code db/clickhouse-init.sql}. */
// Skip when no datasource is configured (context-only tests disable datasources).
@Requires(beans = DataSource.class)
@Singleton
public class ClickHouseSchemaInitializer {

    private final DataSource dataSource;

    public ClickHouseSchemaInitializer(@Named("clickhouse") DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @EventListener
    void onStartup(StartupEvent event) throws Exception {
        String sql;
        try (InputStream in = getClass().getResourceAsStream("/db/clickhouse-init.sql")) {
            if (in == null) {
                throw new IllegalStateException("db/clickhouse-init.sql not found on classpath");
            }
            sql = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        DataSource raw = DelegatingDataSource.unwrapDataSource(dataSource);
        try (Connection conn = raw.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }
}
