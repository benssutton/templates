package com.example.template.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import com.example.template.dto.DataRow;
import com.example.template.dto.DataRowsResponse;
import com.example.template.support.IntegrationSupport;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@MicronautTest
class DataControllerTest extends IntegrationSupport {

    @Inject
    @Client("/")
    HttpClient client;

    @BeforeAll
    void seed() throws Exception {
        String url =
                "jdbc:ch://"
                        + CLICKHOUSE.getHost()
                        + ":"
                        + CLICKHOUSE.getMappedPort(8123)
                        + "/default";
        try (Connection c =
                        DriverManager.getConnection(
                                url, CLICKHOUSE.getUsername(), CLICKHOUSE.getPassword());
                Statement s = c.createStatement()) {
            s.execute(
                    "CREATE TABLE IF NOT EXISTS default.items (id UInt64, name String, value String) ENGINE = MergeTree() ORDER BY id");
            // Two inserts, newest id first: separate parts, so only ORDER BY id guarantees 1, 2.
            s.execute("INSERT INTO default.items VALUES (2,'b','y')");
            s.execute("INSERT INTO default.items VALUES (1,'a','x')");
        }
    }

    @Test
    void getDataReturnsRowsAndTotal() {
        DataRowsResponse r =
                client.toBlocking()
                        .retrieve(HttpRequest.GET("/data?limit=10"), DataRowsResponse.class);
        assertThat(r.total()).isEqualTo(2);
        assertThat(r.limit()).isEqualTo(10);
        assertThat(r.rows()).extracting(DataRow::id).containsExactly(1L, 2L);
    }

    /** FastAPI/Rust parity: an out-of-range limit is 422 {"detail": ...}, not Micronaut's 400. */
    @Test
    void limitOutOfRangeIs422() {
        for (String limit : new String[] {"0", "101"}) {
            HttpClientResponseException ex =
                    catchThrowableOfType(
                            HttpClientResponseException.class,
                            () ->
                                    client.toBlocking()
                                            .exchange(HttpRequest.GET("/data?limit=" + limit)));
            assertThat((Object) ex.getStatus()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
            assertThat(ex.getResponse().getBody(String.class).orElse("")).contains("limit");
        }
    }
}
