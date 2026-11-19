package com.example.template.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.example.template.persistence.analyticsstore.clickhouse.ClickHouseMigrations;
import com.example.template.support.IntegrationSupport;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

/** Schema migrations: Postgres via Flyway, ClickHouse via the app's versioned runner. */
@MicronautTest
class MigrationsTest extends IntegrationSupport {

    @Inject ClickHouseMigrations clickHouse;

    @Inject
    @Named("clickhouse")
    DataSource clickHouseDataSource;

    /**
     * Replicas starting together all migrate the same fresh database; Flyway's advisory lock
     * serialises them, so every run succeeds and V1 is recorded exactly once.
     */
    @Test
    void concurrentPostgresMigrationsOnAFreshDatabaseSucceed() throws Exception {
        String db = "migrate_race_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        try (Connection admin =
                        DriverManager.getConnection(
                                POSTGRES.getJdbcUrl(),
                                POSTGRES.getUsername(),
                                POSTGRES.getPassword());
                Statement stmt = admin.createStatement()) {
            stmt.execute("CREATE DATABASE " + db);
            try {
                PGSimpleDataSource ds = new PGSimpleDataSource();
                ds.setUrl(POSTGRES.getJdbcUrl().replaceFirst("/[^/?]+(\\?|$)", "/" + db + "$1"));
                ds.setUser(POSTGRES.getUsername());
                ds.setPassword(POSTGRES.getPassword());
                Flyway flyway =
                        Flyway.configure().dataSource(ds).locations("classpath:db/postgres").load();

                List<Future<?>> runs = new ArrayList<>();
                try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
                    for (int i = 0; i < 6; i++) {
                        runs.add(pool.submit(flyway::migrate));
                    }
                }
                for (Future<?> run : runs) {
                    assertThatCode(run::get).doesNotThrowAnyException();
                }
                try (Connection c = ds.getConnection();
                        Statement s = c.createStatement();
                        ResultSet rs =
                                s.executeQuery(
                                        "SELECT count(*) FROM flyway_schema_history WHERE version = '1' AND success")) {
                    rs.next();
                    assertThat(rs.getInt(1)).isEqualTo(1);
                }
            } finally {
                stmt.execute("DROP DATABASE IF EXISTS " + db + " WITH (FORCE)");
            }
        }
    }

    /** Startup already applied V1; running again applies nothing and keeps the version. */
    @Test
    void clickHouseMigrationsAreRecordedAndNotReapplied() throws Exception {
        assertThat(clickHouse.migrate()).isEqualTo(1);
        assertThat(clickHouse.migrate()).isEqualTo(1);
        try (Connection c = clickHouseDataSource.getConnection();
                Statement s = c.createStatement();
                ResultSet rs =
                        s.executeQuery("SELECT count() FROM system.tables WHERE name = 'items'")) {
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(1);
        }
    }
}
