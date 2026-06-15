package com.example.template.controller;

import com.example.template.dto.ConfigEntry;
import com.example.template.dto.ConfigSetRequest;
import com.example.template.support.IntegrationSupport;
import io.micronaut.core.type.Argument;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@MicronautTest
class ConfigControllerTest extends IntegrationSupport {

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
        client.toBlocking().exchange(HttpRequest.POST("/config", new ConfigSetRequest("upsertKey", "v1")));
        client.toBlocking().exchange(HttpRequest.POST("/config", new ConfigSetRequest("upsertKey", "v2")));

        List<ConfigEntry> all = client.toBlocking().retrieve(
            HttpRequest.GET("/config"),
            Argument.listOf(ConfigEntry.class));
        assertThat(all.stream().filter(e -> e.key().equals("upsertKey")).map(ConfigEntry::value).toList())
            .containsExactly("v2");
    }
}
