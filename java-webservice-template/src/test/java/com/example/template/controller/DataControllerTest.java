package com.example.template.controller;

import com.example.template.dto.DataRow;
import com.example.template.dto.DataRowsResponse;
import com.example.template.support.IntegrationSupport;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

@MicronautTest
class DataControllerTest extends IntegrationSupport {

    @Inject
    @Client("/")
    HttpClient client;

    @BeforeAll
    void seed() throws Exception {
        String url = "jdbc:ch://" + CLICKHOUSE.getHost() + ":" + CLICKHOUSE.getMappedPort(8123) + "/default";
        try (Connection c = DriverManager.getConnection(url, CLICKHOUSE.getUsername(), CLICKHOUSE.getPassword());
             Statement s = c.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS default.items (id UInt64, name String, value String) ENGINE = MergeTree() ORDER BY id");
            s.execute("INSERT INTO default.items VALUES (1,'a','x'),(2,'b','y')");
        }
    }

    @Test
    void getDataReturnsRowsAndTotal() {
        DataRowsResponse r = client.toBlocking().retrieve(HttpRequest.GET("/data?limit=10"), DataRowsResponse.class);
        assertThat(r.total()).isEqualTo(2);
        assertThat(r.limit()).isEqualTo(10);
        assertThat(r.rows()).extracting(DataRow::id).containsExactly(1L, 2L);
    }
}
