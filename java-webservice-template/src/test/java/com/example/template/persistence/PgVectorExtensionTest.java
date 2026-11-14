package com.example.template.persistence;

import com.example.template.support.IntegrationSupport;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;

import static org.assertj.core.api.Assertions.assertThat;

/** SchemaInitializer runs postgres-init.sql on startup; the vector extension must exist. */
@MicronautTest
class PgVectorExtensionTest extends IntegrationSupport {

    @Inject
    DataSource dataSource;

    @Test
    void vectorExtensionIsInstalled() throws Exception {
        try (Connection c = dataSource.getConnection();
             ResultSet rs = c.createStatement().executeQuery(
                 "SELECT extversion FROM pg_extension WHERE extname = 'vector'")) {
            assertThat(rs.next()).as("vector extension row").isTrue();
        }
    }
}
