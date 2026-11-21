package com.example.template.mcp;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.template.support.IntegrationSupport;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.json.JsonMapper;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Contract test for {@code /mcp}. The official SDK client (pointed at the embedded server) drives
 * the real Streamable HTTP handshake; raw HTTP covers the transport rules a client library would
 * hide (notification status, Host/Origin allow-list).
 */
@MicronautTest
class McpEndpointTest extends IntegrationSupport {

    private static final String ACCEPT = "application/json, text/event-stream";

    @Inject
    @Named("embeddedServer")
    McpSyncClient mcp;

    @Inject
    @Client("/")
    HttpClient client;

    @Inject JsonMapper json;

    @Test
    void sdkClientCompletesHandshakeAndCallsTool() {
        McpSchema.InitializeResult init = mcp.initialize();
        assertThat(init.serverInfo().name()).isEqualTo("java-template");
        assertThat(init.instructions()).startsWith("Tools for this template application.");
        assertThat(init.capabilities().tools()).isNotNull();

        assertThat(mcp.listTools().tools())
                .extracting(McpSchema.Tool::name)
                .containsExactlyInAnyOrder(
                        "get_health_status",
                        "get_config",
                        "get_cache",
                        "get_data",
                        "get_cached_data");

        // The detailed status, same top-level keys as GET /health/status.
        McpSchema.CallToolResult result = call("get_health_status", Map.of());
        assertThat(result.content()).hasSize(1);
        Map<?, ?> viaMcp = (Map<?, ?>) parse(text(result));
        Map<?, ?> viaRest =
                (Map<?, ?>) parse(client.toBlocking().retrieve(HttpRequest.GET("/health/status")));
        assertThat(viaMcp.keySet()).isEqualTo(viaRest.keySet());
    }

    private Object parse(String body) {
        try {
            return json.readValue(body, Object.class);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private McpSchema.CallToolResult call(String tool, Map<String, Object> args) {
        return mcp.callTool(McpSchema.CallToolRequest.builder(tool).arguments(args).build());
    }

    private static String text(McpSchema.CallToolResult result) {
        assertThat(result.isError()).isNotEqualTo(Boolean.TRUE);
        return ((McpSchema.TextContent) result.content().get(0)).text();
    }

    /** Each read tool mirrors its REST endpoint through the same service. */
    @Test
    void readToolsMirrorTheRestEndpoints() {
        mcp.initialize();
        client.toBlocking()
                .exchange(HttpRequest.POST("/config", Map.of("key", "mcp-k", "value", "mcp-v")));
        McpSchema.CallToolResult cfg = call("get_config", Map.of());
        assertThat(cfg.content()).hasSize(1);
        assertThat(text(cfg)).startsWith("[").contains("{\"key\":\"mcp-k\",\"value\":\"mcp-v\"}");
        assertThat(parse(text(cfg)))
                .isEqualTo(parse(client.toBlocking().retrieve(HttpRequest.GET("/config"))));

        client.toBlocking()
                .exchange(
                        HttpRequest.POST(
                                "/cache", Map.of("key", "mcp-c", "value", Map.of("n", 7))));
        assertThat(text(call("get_cache", Map.of("key", "mcp-c"))))
                .isEqualTo("{\"key\":\"mcp-c\",\"value\":{\"n\":7},\"ttl_seconds\":null}");
        McpSchema.CallToolResult missing = call("get_cache", Map.of("key", "mcp-missing"));
        assertThat(missing.isError()).isTrue();
        assertThat(((McpSchema.TextContent) missing.content().get(0)).text())
                .isEqualTo("{\"detail\":\"Key 'mcp-missing' not found\"}");

        assertThat(text(call("get_data", Map.of("limit", 3)))).contains("\"limit\":3");
        assertThat(text(call("get_cached_data", Map.of("limit", 2)))).contains("\"limit\":2");
    }

    /** The spec requires 202 with no body for a notification (a message without an id). */
    @Test
    void notificationIsAcceptedWithoutBody() {
        HttpResponse<String> res =
                client.toBlocking()
                        .exchange(
                                HttpRequest.POST(
                                                "/mcp",
                                                Map.of(
                                                        "jsonrpc",
                                                        "2.0",
                                                        "method",
                                                        "notifications/initialized"))
                                        .contentType(MediaType.APPLICATION_JSON)
                                        .header("Accept", ACCEPT),
                                String.class);
        assertThat((Object) res.getStatus()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(res.getBody()).isEmpty();
    }

    @Test
    void foreignHostIsRejectedWithJsonDetail() {
        assertThat(postPing(Map.of("Host", "evil.example"))).isEqualTo(HttpStatus.FORBIDDEN);
        var request =
                HttpRequest.POST("/mcp", Map.of("jsonrpc", "2.0", "id", 1, "method", "ping"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Accept", ACCEPT)
                        .header("Host", "evil.example");
        HttpClientResponseException e =
                org.assertj.core.api.Assertions.catchThrowableOfType(
                        HttpClientResponseException.class,
                        () -> client.toBlocking().exchange(request, String.class));
        assertThat(e.getResponse().getBody(String.class).orElse(""))
                .isEqualTo("{\"detail\":\"Invalid Host header\"}");
    }

    @Test
    void userinfoOriginIsRejected() {
        assertThat(postPing(Map.of("Origin", "https://localhost:1@evil.example")))
                .isEqualTo(HttpStatus.FORBIDDEN);
    }

    @Test
    void foreignOriginIsRejected() {
        assertThat(postPing(Map.of("Origin", "https://evil.example")))
                .isEqualTo(HttpStatus.FORBIDDEN);
    }

    @Test
    void allowedHostWithoutPortIsAccepted() {
        // HTTPS on 443 sends `Host: localhost` with no port.
        assertThat(postPing(Map.of("Host", "localhost", "Origin", "https://localhost")))
                .isEqualTo(HttpStatus.OK);
    }

    /** Object, not HttpStatus: HttpStatus is a CharSequence, so assertThat would be ambiguous. */
    private Object postPing(Map<String, String> headers) {
        var request =
                HttpRequest.POST("/mcp", Map.of("jsonrpc", "2.0", "id", 1, "method", "ping"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("Accept", ACCEPT);
        headers.forEach(request::header);
        try {
            return client.toBlocking().exchange(request, String.class).getStatus();
        } catch (HttpClientResponseException e) {
            return e.getStatus();
        }
    }
}
