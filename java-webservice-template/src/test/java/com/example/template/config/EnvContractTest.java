package com.example.template.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    void postgresUrlWithoutPortDefaultsTo5432() {
        assertThat(EnvContract.translate(Map.of("POSTGRES_URL", "postgresql://u:p@db/appdb")))
                .containsEntry("datasources.default.url", "jdbc:postgresql://db:5432/appdb");
    }

    @Test
    void postgresUrlQueryStringIsKept() {
        assertThat(
                        EnvContract.translate(
                                Map.of(
                                        "POSTGRES_URL",
                                        "postgresql://u:p@db:5432/appdb?sslmode=require")))
                .containsEntry(
                        "datasources.default.url",
                        "jdbc:postgresql://db:5432/appdb?sslmode=require");
    }

    @Test
    void postgresUrlWithoutUserinfoSetsNoCredentials() {
        assertThat(EnvContract.translate(Map.of("POSTGRES_URL", "postgresql://db:5432/appdb")))
                .containsEntry("datasources.default.url", "jdbc:postgresql://db:5432/appdb")
                .doesNotContainKeys("datasources.default.username", "datasources.default.password");
    }

    @Test
    void blankValuesAreTreatedAsAbsent() {
        assertThat(
                        EnvContract.translate(
                                Map.of(
                                        "SERVER_HOST", "",
                                        "POSTGRES_URL", " ",
                                        "CLICKHOUSE_HOST", "",
                                        "POSTGRES_ACQUIRE_TIMEOUT_SECONDS", "")))
                .isEmpty();
    }

    @Test
    void blankServerHostSetsNothing() {
        assertThat(EnvContract.translate(Map.of("SERVER_HOST", "  "))).isEmpty();
    }

    @Test
    void clickhouseHostAloneUsesDefaultPortAndDatabase() {
        assertThat(EnvContract.translate(Map.of("CLICKHOUSE_HOST", "ch")))
                .containsEntry("datasources.clickhouse.url", "jdbc:ch://ch:8123/default");
    }

    @Test
    void blankClickhousePortAndDatabaseUseDefaults() {
        assertThat(
                        EnvContract.translate(
                                Map.of(
                                        "CLICKHOUSE_HOST", "ch",
                                        "CLICKHOUSE_PORT", "",
                                        "CLICKHOUSE_DATABASE", "")))
                .containsEntry("datasources.clickhouse.url", "jdbc:ch://ch:8123/default");
    }

    @Test
    void badAcquireTimeoutNamesTheVariableAndValue() {
        assertThatThrownBy(() ->
                        EnvContract.translate(Map.of("POSTGRES_ACQUIRE_TIMEOUT_SECONDS", "soon")))
                .isInstanceOf(IllegalArgumentException.class)
                .isNotInstanceOf(NumberFormatException.class)
                .hasMessageContaining("POSTGRES_ACQUIRE_TIMEOUT_SECONDS")
                .hasMessageContaining("soon");
    }

    @Test
    void absentNamesAddNothing() {
        assertThat(EnvContract.translate(Map.of())).isEmpty();
    }

    @Test
    void issuerAndAudienceBecomeClaimsValidatorsOnlyWhenSet() {
        assertThat(
                        EnvContract.translate(
                                Map.of("AUTH_JWT_ISSUER", "https://i", "AUTH_JWT_AUDIENCE", "api")))
                .containsEntry("micronaut.security.token.jwt.claims-validators.issuer", "https://i")
                .containsEntry("micronaut.security.token.jwt.claims-validators.audience", "api");
        assertThat(EnvContract.translate(Map.of("AUTH_JWT_ISSUER", "", "AUTH_JWT_AUDIENCE", " ")))
                .isEmpty();
    }
}
