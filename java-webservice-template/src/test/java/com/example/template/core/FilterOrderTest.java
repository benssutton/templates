package com.example.template.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.template.support.IntegrationSupport;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Correlation and timing wrap every other filter, so even a rejected request carries both. */
@MicronautTest
class FilterOrderTest extends IntegrationSupport {

    @Inject
    @Client("/")
    HttpClient client;

    @Test
    void mcpHostRejectionStillCarriesCorrelationIdAndServerTiming() {
        var request =
                HttpRequest.POST("/mcp", Map.of("jsonrpc", "2.0", "id", 1, "method", "ping"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Accept", "application/json, text/event-stream")
                        .header("Host", "evil.example")
                        .header("X-Request-ID", "order-test-1");
        HttpClientResponseException e =
                org.assertj.core.api.Assertions.catchThrowableOfType(
                        HttpClientResponseException.class,
                        () -> client.toBlocking().exchange(request, String.class));
        HttpResponse<?> response = e.getResponse();
        assertThat(response.code()).isEqualTo(403);
        assertThat(response.getHeaders().get("X-Request-ID")).isEqualTo("order-test-1");
        assertThat(response.getHeaders().get("Server-Timing")).isNotBlank();
    }
}
