package com.example.template.persistence.analyticsstore.clickhouse;

import com.example.template.config.AppSettings;
import com.example.template.core.Retry;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.data.connection.jdbc.advice.DelegatingDataSource;
import io.micronaut.runtime.event.annotation.EventListener;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Versioned ClickHouse schema, applied by the app at startup (Flyway covers Postgres; it has no
 * first-party ClickHouse support). Migrations are {@code db/clickhouse/V1.sql}, {@code V2.sql}, ...
 * (one statement each); applied versions are recorded in {@code schema_migrations} and only newer
 * ones run.
 *
 * <p>ClickHouse has no transactional DDL or advisory lock, so two replicas starting together may
 * both run a migration. Each migration must therefore be idempotent ({@code CREATE ... IF NOT
 * EXISTS}, {@code ADD COLUMN IF NOT EXISTS}); a duplicate history row is harmless because the
 * highest version wins.
 */
@Singleton
@Requires(beans = DataSource.class)
public class ClickHouseMigrations {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseMigrations.class);
    private static final String HISTORY_DDL =
            "CREATE TABLE IF NOT EXISTS schema_migrations "
                    + "(version UInt32, applied_at DateTime DEFAULT now()) ENGINE = MergeTree ORDER BY version";

    private final DataSource dataSource;
    private final AppSettings settings;

    public ClickHouseMigrations(@Named("clickhouse") DataSource dataSource, AppSettings settings) {
        this.dataSource = dataSource;
        this.settings = settings;
    }

    /** Retried with backoff, so it does not depend on ClickHouse already being up. */
    @EventListener
    void onStartup(StartupEvent event) throws Exception {
        Retry.connectWithBackoff(
                () -> {
                    migrate();
                    return null;
                },
                "ClickHouse migrations",
                settings.connectMaxAttempts(),
                settings.connectBaseDelaySeconds(),
                settings.connectMaxDelaySeconds());
    }

    /** Applies every migration newer than the recorded version; returns the version reached. */
    public int migrate() throws SQLException {
        // Outside any @Transactional scope, so unwrap Micronaut Data's contextual proxy.
        DataSource raw = DelegatingDataSource.unwrapDataSource(dataSource);
        try (Connection conn = raw.getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute(HISTORY_DDL);
            int current = currentVersion(stmt);
            int version = current;
            String sql;
            while ((sql = load(version + 1)) != null) {
                version++;
                stmt.execute(sql);
                stmt.execute("INSERT INTO schema_migrations (version) VALUES (" + version + ")");
                LOG.info("ClickHouse: applied migration V{}", version);
            }
            if (version == current) {
                LOG.info("ClickHouse: schema up to date at V{}", current);
            }
            return version;
        }
    }

    private static int currentVersion(Statement stmt) throws SQLException {
        try (ResultSet rs = stmt.executeQuery("SELECT max(version) FROM schema_migrations")) {
            return rs.next() ? rs.getInt(1) : 0;
        }
    }

    private static String load(int version) {
        try (InputStream in =
                ClickHouseMigrations.class.getResourceAsStream(
                        "/db/clickhouse/V" + version + ".sql")) {
            return in == null ? null : new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
