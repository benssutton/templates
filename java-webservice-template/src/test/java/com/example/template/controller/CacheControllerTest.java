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
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import redis.clients.jedis.RedisClient;

@MicronautTest
class CacheControllerTest extends IntegrationSupport {

    @Inject
    @Client("/")
    HttpClient client;

    @Test
    void setThenGetRoundTripsJson() {
        String key = unique("cache");
        client.toBlocking()
                .exchange(
                        HttpRequest.POST(
                                "/cache", Map.of("key", key, "value", Map.of("a", 1, "b", "two"))));
        CacheEntry got =
                client.toBlocking().retrieve(HttpRequest.GET("/cache/" + key), CacheEntry.class);
        assertThat(got.key()).isEqualTo(key);
        assertThat(got.value().get("b").getStringValue()).isEqualTo("two");
        assertThat(got.value().get("a").getNumberValue()).isEqualTo(1);
    }

    /** An empty key or a TTL below 1 is 422 and stores nothing. */
    static Stream<Map<String, Object>> invalidEntries() {
        return Stream.of(
                // EXPIRE 0 deletes the key
                Map.of("key", "bad-ttl", "value", 1, "ttl_seconds", 0),
                Map.of("key", "bad-ttl", "value", 1, "ttl_seconds", -1),
                Map.of("key", "", "value", 1));
    }

    @ParameterizedTest
    @MethodSource("invalidEntries")
    void invalidKeyOrTtlIs422(Map<String, Object> body) {
        HttpClientResponseException ex =
                catchThrowableOfType(
                        HttpClientResponseException.class,
                        () -> client.toBlocking().exchange(HttpRequest.POST("/cache", body)));
        assertThat((Object) ex.getStatus()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(ex.getResponse().getBody(String.class).orElse("")).contains("\"detail\"");
    }

    @Test
    void rejectedEntryStoresNothing() {
        HttpClientResponseException missing =
                catchThrowableOfType(
                        HttpClientResponseException.class,
                        () -> client.toBlocking().exchange(HttpRequest.GET("/cache/bad-ttl")));
        assertThat((Object) missing.getStatus()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void ttlIsAppliedWithTheValue() {
        String key = unique("ttl");
        client.toBlocking()
                .exchange(
                        HttpRequest.POST(
                                "/cache", Map.of("key", key, "value", "v", "ttl_seconds", 60)));
        CacheEntry got =
                client.toBlocking().retrieve(HttpRequest.GET("/cache/" + key), CacheEntry.class);
        assertThat(got.ttlSeconds()).isBetween(1, 60);
    }

    @Inject RedisClient jedis;

    /** JSON.SET on a key holding a plain string fails inside EXEC; that must not read as 201. */
    @Test
    void setOnKeyOfWrongTypeIsAnError() {
        String key = unique("plain");
        jedis.set(key, "x");
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
                                                                key,
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
