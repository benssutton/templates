package com.example.template.controller;

import static org.assertj.core.api.Assertions.assertThat;

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
import java.util.List;
import org.junit.jupiter.api.Test;

@MicronautTest
class ConfigControllerTest extends IntegrationSupport {

    @Inject
    @Client("/")
    HttpClient client;

    @Test
    void postReturns201AndGetListsTheEntry() {
        String key = unique("feature");
        HttpResponse<ConfigEntry> created =
                client.toBlocking()
                        .exchange(
                                HttpRequest.POST("/config", new ConfigSetRequest(key, "on")),
                                ConfigEntry.class);
        // Micronaut 5: HttpStatus implements CharSequence, so the bare assertThat(...) overload
        // is ambiguous against AssertJ's assertThat(CharSequence). Cast to Object to select the
        // generic ObjectAssert overload while keeping the equality contract intact.
        assertThat((Object) created.getStatus()).isEqualTo(HttpStatus.CREATED);
        assertThat(created.body()).isEqualTo(new ConfigEntry(key, "on"));

        List<ConfigEntry> all =
                client.toBlocking()
                        .retrieve(HttpRequest.GET("/config"), Argument.listOf(ConfigEntry.class));
        assertThat(all).contains(new ConfigEntry(key, "on"));
    }

    @Test
    void postIsUpsert() {
        String key = unique("upsert");
        client.toBlocking().exchange(HttpRequest.POST("/config", new ConfigSetRequest(key, "v1")));
        client.toBlocking().exchange(HttpRequest.POST("/config", new ConfigSetRequest(key, "v2")));

        List<ConfigEntry> all =
                client.toBlocking()
                        .retrieve(HttpRequest.GET("/config"), Argument.listOf(ConfigEntry.class));
        assertThat(all.stream().filter(e -> e.key().equals(key)).map(ConfigEntry::value).toList())
                .containsExactly("v2");
    }
}
