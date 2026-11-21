package com.example.template.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

class EnvContractTest {

    @Test
    void postgresUrlBecomesJdbcUrlAndCredentials() {
        Map<String, Object> p =
                EnvContract.translate(
                        Map.of("POSTGRES_URL", "postgresql://u:p%40ss@db:5432/appdb"));
        assertThat(p)
                .containsEntry("datasources.default.url", "jdbc:postgresql://db:5432/appdb")
                .containsEntry("datasources.default.username", "u")
                .containsEntry("datasources.default.password", "p@ss");
    }

    @Test
    void clickhouseHostPortDatabaseBecomeJdbcUrl() {
        Map<String, Object> p =
                EnvContract.translate(
                        Map.of(
                                "CLICKHOUSE_HOST",
                                "ch",
                                "CLICKHOUSE_PORT",
                                "8123",
                                "CLICKHOUSE_DATABASE",
                                "analytics"));
        assertThat(p).containsEntry("datasources.clickhouse.url", "jdbc:ch://ch:8123/analytics");
    }

    @Test
    void acquireTimeoutSecondsBecomesMillis() {
        assertThat(EnvContract.translate(Map.of("POSTGRES_ACQUIRE_TIMEOUT_SECONDS", "2.5")))
                .containsEntry("datasources.default.connection-timeout", "2500");
    }

    @Test
    void serverHostIsPassedThrough() {
        assertThat(EnvContract.translate(Map.of("SERVER_HOST", "127.0.0.1")))
                .containsEntry("micronaut.server.host", "127.0.0.1");
    }

    @Test
    void absentNamesAddNothing() {
        assertThat(EnvContract.translate(Map.of())).isEmpty();
    }
}
