package com.example.template.mcp;

import com.example.template.support.IntegrationSupport;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Contract test for the MCP endpoint at {@code /mcp}. Exercises the real route over HTTP
 * against the shared Postgres/ClickHouse/Redis containers (every full-app test does), so
 * {@code get_health_status} resolves through the real {@code HealthService}.
 */
@MicronautTest
class McpEndpointTest extends IntegrationSupport {

    @Inject
    @Client("/")
    HttpClient client;

    /** The /mcp route is mounted: a GET hits the controller (405), not the 404 router miss. */
    @Test
    void routeIsMounted() {
        HttpClientResponseException ex = catchThrowableOfType(
            () -> client.toBlocking().exchange(HttpRequest.GET("/mcp")),
            HttpClientResponseException.class);
        assertThat((Object) ex.getStatus()).isNotEqualTo(HttpStatus.NOT_FOUND);
        assertThat((Object) ex.getStatus()).isEqualTo(HttpStatus.METHOD_NOT_ALLOWED);
    }

    @Test
    @SuppressWarnings("unchecked")
    void toolsListAdvertisesGetHealthStatus() {
        Map<String, Object> body = client.toBlocking().retrieve(
            HttpRequest.POST("/mcp", Map.of(
                "jsonrpc", "2.0", "id", 1, "method", "tools/list", "params", Map.of())),
            Map.class);

        assertThat(body).containsEntry("jsonrpc", "2.0");
        Map<String, Object> result = (Map<String, Object>) body.get("result");
        List<Map<String, Object>> tools = (List<Map<String, Object>>) result.get("tools");
        assertThat(tools).hasSize(1);
        assertThat(tools.get(0)).containsEntry("name", "get_health_status");
        assertThat(tools.get(0).get("inputSchema")).isInstanceOf(Map.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    void toolsCallReturnsHealthStatus() {
        Map<String, Object> body = client.toBlocking().retrieve(
            HttpRequest.POST("/mcp", Map.of(
                "jsonrpc", "2.0", "id", 2, "method", "tools/call",
                "params", Map.of("name", "get_health_status", "arguments", Map.of()))),
            Map.class);

        Map<String, Object> result = (Map<String, Object>) body.get("result");
        List<Map<String, Object>> content = (List<Map<String, Object>>) result.get("content");
        assertThat(content).hasSize(1);
        assertThat(content.get(0)).containsEntry("type", "text");
        // Default app status is "running" (com.example.template.config.AppSettings).
        assertThat(content.get(0)).containsEntry("text", "running");
    }

    @Test
    @SuppressWarnings("unchecked")
    void initializeReturnsServerInfoAndCapabilities() {
        Map<String, Object> body = client.toBlocking().retrieve(
            HttpRequest.POST("/mcp", Map.of(
                "jsonrpc", "2.0", "id", 3, "method", "initialize", "params", Map.of())),
            Map.class);

        Map<String, Object> result = (Map<String, Object>) body.get("result");
        assertThat(result).containsKey("protocolVersion");
        Map<String, Object> serverInfo = (Map<String, Object>) result.get("serverInfo");
        assertThat(serverInfo).containsEntry("name", "java-template");
        Map<String, Object> capabilities = (Map<String, Object>) result.get("capabilities");
        assertThat(capabilities).containsKey("tools");
    }

    @Test
    @SuppressWarnings("unchecked")
    void unknownMethodReturnsJsonRpcError() {
        Map<String, Object> body = client.toBlocking().retrieve(
            HttpRequest.POST("/mcp", Map.of(
                "jsonrpc", "2.0", "id", 4, "method", "does/not/exist", "params", Map.of())),
            Map.class);

        assertThat(body.get("result")).isNull();
        Map<String, Object> error = (Map<String, Object>) body.get("error");
        assertThat(error).containsEntry("code", -32601);
    }
}
