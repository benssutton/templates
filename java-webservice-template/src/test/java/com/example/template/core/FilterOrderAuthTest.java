package com.example.template.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import com.example.template.support.IntegrationSupport;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** With auth on, the MCP Host check still runs before security: a foreign Host is 403, not 401. */
@MicronautTest
class FilterOrderAuthTest extends IntegrationSupport {

    @Inject
    @Client("/")
    HttpClient client;

    @Override
    public Map<String, String> getProperties() {
        Map<String, String> props = super.getProperties();
        props.put("template.auth.enabled", "true");
        props.put("template.auth.jwt-secret", "filter-order-secret-that-is-at-least-32-bytes");
        return props;
    }

    private static MutableHttpRequest<?> unauthenticatedPing() {
        return HttpRequest.POST("/mcp", Map.of("jsonrpc", "2.0", "id", 1, "method", "ping"))
                .contentType(MediaType.APPLICATION_JSON)
                .header("Accept", "application/json, text/event-stream");
    }

    private int status(MutableHttpRequest<?> request) {
        HttpClientResponseException e =
                catchThrowableOfType(
                        HttpClientResponseException.class,
                        () -> client.toBlocking().exchange(request, String.class));
        HttpResponse<?> response = e.getResponse();
        return response.code();
    }

    @Test
    void foreignHostIs403BeforeAuthentication() {
        assertThat(status(unauthenticatedPing().header("Host", "evil.example"))).isEqualTo(403);
    }

    /** Control: auth really is on, so an allowed Host without a token is 401. */
    @Test
    void allowedHostWithoutTokenIs401() {
        assertThat(status(unauthenticatedPing())).isEqualTo(401);
    }
}
