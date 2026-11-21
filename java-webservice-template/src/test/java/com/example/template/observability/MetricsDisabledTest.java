package com.example.template.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import com.example.template.support.IntegrationSupport;
import io.micronaut.context.annotation.Property;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

/** METRICS_ENABLED=false: the service starts and /metrics is absent, as in the Python template. */
@MicronautTest
@Property(name = "micronaut.metrics.enabled", value = "false")
class MetricsDisabledTest extends IntegrationSupport {

    @Inject
    @Client("/")
    HttpClient client;

    @Test
    void metricsIsNotFoundWhenDisabled() {
        HttpClientResponseException e =
                catchThrowableOfType(
                        HttpClientResponseException.class,
                        () -> client.toBlocking().retrieve(HttpRequest.GET("/metrics")));
        assertThat((Object) e.getStatus()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void restSurfaceStillWorks() {
        assertThat(client.toBlocking().exchange(HttpRequest.GET("/health/live")).code())
                .isEqualTo(200);
    }
}
