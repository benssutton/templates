package com.example.template.controller;

import com.example.template.dto.CacheEntry;
import com.example.template.dto.CacheSetRequest;
import com.example.template.support.IntegrationSupport;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

@MicronautTest
class CacheControllerTest extends IntegrationSupport {

    @Inject
    @Client("/")
    HttpClient client;

    @Test
    void setThenGetRoundTripsJson() {
        client.toBlocking().exchange(HttpRequest.POST("/cache",
            new CacheSetRequest("k1", Map.of("a", 1, "b", "two"), null)));
        CacheEntry got = client.toBlocking().retrieve(HttpRequest.GET("/cache/k1"), CacheEntry.class);
        assertThat(got.key()).isEqualTo("k1");
        @SuppressWarnings("unchecked")
        Map<String, Object> value = (Map<String, Object>) got.value();
        assertThat(value).containsEntry("b", "two");
    }

    @Test
    void missingKeyReturns404() {
        HttpClientResponseException ex = catchThrowableOfType(
            () -> client.toBlocking().exchange(HttpRequest.GET("/cache/nope")),
            HttpClientResponseException.class);
        assertThat((Object) ex.getStatus()).isEqualTo(HttpStatus.NOT_FOUND);
    }
}
