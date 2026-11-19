package com.example.template.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.template.support.IntegrationSupport;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.sql.Connection;
import java.sql.ResultSet;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

/** Flyway applies db/postgres/V1 on startup, which enables the vector extension. */
@MicronautTest
class PgVectorExtensionTest extends IntegrationSupport {

    @Inject DataSource dataSource;

    @Test
    void vectorExtensionIsInstalled() throws Exception {
        try (Connection c = dataSource.getConnection();
                ResultSet rs =
                        c.createStatement()
                                .executeQuery(
                                        "SELECT extversion FROM pg_extension WHERE extname = 'vector'")) {
            assertThat(rs.next()).as("vector extension row").isTrue();
        }
    }
}
