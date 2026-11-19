package com.example.template.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import com.example.template.dto.CacheEntry;
import com.example.template.support.IntegrationSupport;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.RedisClient;

@MicronautTest
class CacheControllerTest extends IntegrationSupport {

    @Inject
    @Client("/")
    HttpClient client;

    @Test
    void setThenGetRoundTripsJson() {
        client.toBlocking()
                .exchange(
                        HttpRequest.POST(
                                "/cache",
                                Map.of("key", "k1", "value", Map.of("a", 1, "b", "two"))));
        CacheEntry got =
                client.toBlocking().retrieve(HttpRequest.GET("/cache/k1"), CacheEntry.class);
        assertThat(got.key()).isEqualTo("k1");
        assertThat(got.value().get("b").getStringValue()).isEqualTo("two");
        assertThat(got.value().get("a").getNumberValue()).isEqualTo(1);
    }

    /** Python/Rust parity: an empty key or a TTL below 1 is 422 and stores nothing. */
    @Test
    void invalidKeyOrTtlIs422() {
        List<Map<String, Object>> invalid =
                List.of(
                        Map.of(
                                "key",
                                "bad-ttl",
                                "value",
                                1,
                                "ttl_seconds",
                                0), // EXPIRE 0 deletes the key
                        Map.of("key", "bad-ttl", "value", 1, "ttl_seconds", -1),
                        Map.of("key", "", "value", 1));
        for (Map<String, Object> body : invalid) {
            HttpClientResponseException ex =
                    catchThrowableOfType(
                            HttpClientResponseException.class,
                            () -> client.toBlocking().exchange(HttpRequest.POST("/cache", body)));
            assertThat((Object) ex.getStatus())
                    .as("%s", body)
                    .isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
            assertThat(ex.getResponse().getBody(String.class).orElse("")).contains("\"detail\"");
        }
        HttpClientResponseException missing =
                catchThrowableOfType(
                        HttpClientResponseException.class,
                        () -> client.toBlocking().exchange(HttpRequest.GET("/cache/bad-ttl")));
        assertThat((Object) missing.getStatus()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void ttlIsAppliedWithTheValue() {
        client.toBlocking()
                .exchange(
                        HttpRequest.POST(
                                "/cache", Map.of("key", "k-ttl", "value", "v", "ttl_seconds", 60)));
        CacheEntry got =
                client.toBlocking().retrieve(HttpRequest.GET("/cache/k-ttl"), CacheEntry.class);
        assertThat(got.ttlSeconds()).isBetween(1, 60);
    }

    @Inject RedisClient jedis;

    /** JSON.SET on a key holding a plain string fails inside EXEC; that must not read as 201. */
    @Test
    void setOnKeyOfWrongTypeIsAnError() {
        jedis.set("plain-string-key", "x");
        HttpClientResponseException ex =
                catchThrowableOfType(
                        HttpClientResponseException.class,
                        () ->
                                client.toBlocking()
                                        .exchange(
                                                HttpRequest.POST(
                                                        "/cache",
                                                        Map.of(
                                                                "key",
                                                                "plain-string-key",
                                                                "value",
                                                                Map.of("a", 1),
                                                                "ttl_seconds",
                                                                60))));
        assertThat(ex).isNotNull();
        assertThat((Object) ex.getStatus()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @Test
    void missingKeyReturns404() {
        HttpClientResponseException ex =
                catchThrowableOfType(
                        () -> client.toBlocking().exchange(HttpRequest.GET("/cache/nope")),
                        HttpClientResponseException.class);
        assertThat((Object) ex.getStatus()).isEqualTo(HttpStatus.NOT_FOUND);
    }
}
