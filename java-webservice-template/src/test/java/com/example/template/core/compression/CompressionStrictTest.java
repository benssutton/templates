package com.example.template.core.compression;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.template.support.IntegrationSupport;
import io.micronaut.context.annotation.Property;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

@MicronautTest
@Property(name = "template.compression.require-zstd", value = "true")
class CompressionStrictTest extends IntegrationSupport {

    @Inject EmbeddedServer server;

    private final HttpClient http = HttpClient.newHttpClient();

    private HttpResponse<String> send(String method, String path, String... headers)
            throws Exception {
        HttpRequest.Builder b =
                HttpRequest.newBuilder(server.getURI().resolve(path))
                        .method(method, HttpRequest.BodyPublishers.noBody());
        if (headers.length > 0) {
            b.headers(headers);
        }
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    @Test
    void clientsWithoutZstdGet406Uncompressed() throws Exception {
        List<String[]> cases =
                List.of(
                        new String[0],
                        new String[] {"Accept-Encoding", "identity"},
                        new String[] {"Accept-Encoding", "gzip"},
                        new String[] {"Accept-Encoding", "zstd;q=0"});
        for (String[] headers : cases) {
            HttpResponse<String> r = send("GET", "/", headers);
            assertThat(r.statusCode()).as(Arrays.toString(headers)).isEqualTo(406);
            assertThat(r.headers().firstValue("Content-Encoding")).isEmpty();
            assertThat(r.body()).isEqualTo("{\"detail\":\"Accept-Encoding must allow zstd\"}");
        }
    }

    @Test
    void probesMetricsMcpAndPreflightAreExempt() throws Exception {
        for (String path : List.of("/health/live", "/health/ready", "/metrics")) {
            assertThat(send("GET", path, "Accept-Encoding", "identity").statusCode())
                    .as(path)
                    .isNotEqualTo(406);
        }
        HttpRequest ping =
                HttpRequest.newBuilder(server.getURI().resolve("/mcp"))
                        .header("Accept-Encoding", "identity")
                        .header("Accept", "application/json, text/event-stream")
                        .header("Content-Type", "application/json")
                        .POST(
                                HttpRequest.BodyPublishers.ofString(
                                        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}"))
                        .build();
        assertThat(http.send(ping, HttpResponse.BodyHandlers.ofString()).statusCode())
                .isNotEqualTo(406);
        HttpResponse<String> preflight =
                send(
                        "OPTIONS",
                        "/config",
                        "Origin",
                        "https://app.example.com",
                        "Access-Control-Request-Method",
                        "GET");
        assertThat(preflight.statusCode()).isEqualTo(200);
    }

    @Test
    void responsesBelowTheThresholdAreCompressed() throws Exception {
        HttpResponse<String> r = send("GET", "/", "Accept-Encoding", "zstd");
        assertThat(r.statusCode()).isEqualTo(200);
        assertThat(r.headers().firstValue("Content-Encoding")).contains("zstd");
    }
}
