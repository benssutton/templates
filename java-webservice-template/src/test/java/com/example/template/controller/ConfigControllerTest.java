package com.example.template.controller;

import com.example.template.dto.ConfigEntry;
import com.example.template.dto.ConfigSetRequest;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.micronaut.test.support.TestPropertyProvider;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@MicronautTest
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConfigControllerTest implements TestPropertyProvider {

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
        new PostgreSQLContainer<>("postgres:16-alpine");

    @Override
    public Map<String, String> getProperties() {
        if (!POSTGRES.isRunning()) {
            POSTGRES.start();
        }
        return Map.of(
            "datasources.default.url", POSTGRES.getJdbcUrl(),
            "datasources.default.username", POSTGRES.getUsername(),
            "datasources.default.password", POSTGRES.getPassword()
        );
    }

    @Inject
    @Client("/")
    HttpClient client;

    @Test
    void postReturns201AndGetListsTheEntry() {
        HttpResponse<ConfigEntry> created = client.toBlocking().exchange(
            HttpRequest.POST("/config", new ConfigSetRequest("featureX", "on")),
            ConfigEntry.class);
        // Micronaut 5: HttpStatus implements CharSequence, so the bare assertThat(...) overload
        // is ambiguous against AssertJ's assertThat(CharSequence). Cast to Object to select the
        // generic ObjectAssert overload while keeping the equality contract intact.
        assertThat((Object) created.getStatus()).isEqualTo(HttpStatus.CREATED);
        assertThat(created.body()).isEqualTo(new ConfigEntry("featureX", "on"));

        List<ConfigEntry> all = client.toBlocking().retrieve(
            HttpRequest.GET("/config"),
            Argument.listOf(ConfigEntry.class));
        assertThat(all).contains(new ConfigEntry("featureX", "on"));
    }

    @Test
    void postIsUpsert() {
        client.toBlocking().exchange(HttpRequest.POST("/config", new ConfigSetRequest("k", "v1")));
        client.toBlocking().exchange(HttpRequest.POST("/config", new ConfigSetRequest("k", "v2")));

        List<ConfigEntry> all = client.toBlocking().retrieve(
            HttpRequest.GET("/config"),
            Argument.listOf(ConfigEntry.class));
        assertThat(all.stream().filter(e -> e.key().equals("k")).map(ConfigEntry::value).toList())
            .containsExactly("v2");
    }
}
