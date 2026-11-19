package com.example.template.controller;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.template.support.IntegrationSupport;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * A store that freezes mid-request (a paused container: connections stay open, nothing answers)
 * yields 503 {"detail": "Service Unavailable"} within the client timeouts, not a hung request or a
 * 500. Uses its own Postgres and Redis so pausing them cannot disturb other test classes.
 */
@MicronautTest
class StoreOutageTest extends IntegrationSupport {

    private static final Duration BOUND = Duration.ofSeconds(10);
    private static final String UNAVAILABLE = "{\"detail\":\"Service Unavailable\"}";

    static final PostgreSQLContainer<?> OWN_POSTGRES =
            new PostgreSQLContainer<>(
                    DockerImageName.parse("pgvector/pgvector:0.8.4-pg18")
                            .asCompatibleSubstituteFor("postgres"));
    static final GenericContainer<?> OWN_REDIS =
            new GenericContainer<>(DockerImageName.parse("redis/redis-stack-server:7.4.0-v8"))
                    .withExposedPorts(6379);

    static {
        OWN_POSTGRES.start();
        OWN_REDIS.start();
    }

    @Inject
    @Client("/")
    HttpClient client;

    @Override
    public Map<String, String> getProperties() {
        Map<String, String> props = super.getProperties();
        props.put("datasources.default.url", OWN_POSTGRES.getJdbcUrl());
        props.put("datasources.default.username", OWN_POSTGRES.getUsername());
        props.put("datasources.default.password", OWN_POSTGRES.getPassword());
        props.put("datasources.default.data-source-properties.socketTimeout", "3");
        props.put(
                "redis.uri",
                "redis://" + OWN_REDIS.getHost() + ":" + OWN_REDIS.getMappedPort(6379) + "/0");
        return props;
    }

    record Res(int status, String body, Duration took) {}

    private Res timed(MutableHttpRequest<?> request) {
        long start = System.nanoTime();
        int status;
        String body;
        try {
            var r = client.toBlocking().exchange(request, String.class);
            status = r.code();
            body = r.getBody().orElse("");
        } catch (HttpClientResponseException e) {
            status = e.code();
            body = e.getResponse().getBody(String.class).orElse("");
        }
        return new Res(status, body, Duration.ofNanos(System.nanoTime() - start));
    }

    private static void pause(GenericContainer<?> c) {
        DockerClientFactory.instance().client().pauseContainerCmd(c.getContainerId()).exec();
    }

    private static void unpause(GenericContainer<?> c) {
        DockerClientFactory.instance().client().unpauseContainerCmd(c.getContainerId()).exec();
    }

    @Test
    void frozenPostgresIsABounded503() {
        assertThat(timed(HttpRequest.GET("/config")).status()).isEqualTo(200); // pool warmed
        pause(OWN_POSTGRES);
        try {
            Res res = timed(HttpRequest.GET("/config"));
            assertThat(res.status()).as(res.body()).isEqualTo(503);
            assertThat(res.body()).isEqualTo(UNAVAILABLE);
            assertThat(res.took()).isLessThan(BOUND);
        } finally {
            unpause(OWN_POSTGRES);
        }
    }

    @Test
    void frozenRedisIsABounded503() {
        timed(HttpRequest.POST("/cache", Map.of("key", "outage-k", "value", 1)));
        pause(OWN_REDIS);
        try {
            Res res = timed(HttpRequest.GET("/cache/outage-k"));
            assertThat(res.status()).as(res.body()).isEqualTo(503);
            assertThat(res.body()).isEqualTo(UNAVAILABLE);
            assertThat(res.took()).isLessThan(BOUND);
        } finally {
            unpause(OWN_REDIS);
        }
    }
}
