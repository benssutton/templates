package com.example.template.controller;

import com.example.template.dto.health.DetailedStatusResponse;
import com.example.template.dto.health.LivenessResponse;
import com.example.template.dto.health.ReadinessResponse;
import com.example.template.support.PostgresTestSupport;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@MicronautTest
class HealthControllerTest extends PostgresTestSupport {

    @Inject
    @Client("/")
    HttpClient client;

    @Test
    void liveIsAlive() {
        LivenessResponse r = client.toBlocking().retrieve(HttpRequest.GET("/health/live"), LivenessResponse.class);
        assertThat(r.status()).isEqualTo("alive");
        assertThat(r.uptimeSeconds()).isGreaterThanOrEqualTo(0.0);
    }

    @Test
    void readyReportsPostgresUp() {
        ReadinessResponse r = client.toBlocking().retrieve(HttpRequest.GET("/health/ready"), ReadinessResponse.class);
        assertThat(r.checks()).anyMatch(c -> c.name().equals("postgres") && c.status().equals("up"));
    }

    @Test
    void statusReportsAppAndSystem() {
        DetailedStatusResponse r = client.toBlocking().retrieve(HttpRequest.GET("/health/status"), DetailedStatusResponse.class);
        assertThat(r.app().status()).isEqualTo("running");
        assertThat(r.system().host().memoryTotalBytes()).isGreaterThan(0);
        assertThat(r.dependencies()).anyMatch(d -> d.name().equals("postgres"));
    }
}
