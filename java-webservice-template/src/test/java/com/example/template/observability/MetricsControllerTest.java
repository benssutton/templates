package com.example.template.observability;

import com.example.template.support.PostgresTestSupport;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@MicronautTest
class MetricsControllerTest extends PostgresTestSupport {

    @Inject
    @Client("/")
    HttpClient client;

    @Test
    void metricsExposesPrometheusTextWithCustomGauges() {
        String body = client.toBlocking().retrieve(HttpRequest.GET("/metrics"));
        assertThat(body).contains("dependency_up");
        assertThat(body).contains("jvm_memory_used_bytes"); // standard Micrometer binder
    }
}
