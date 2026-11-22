package com.example.template.core.compression;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.template.support.IntegrationSupport;
import com.github.luben.zstd.Zstd;
import io.micronaut.context.annotation.Property;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/** With response compression off, request bodies are still decoded. */
@MicronautTest
@Property(name = "template.compression.enabled", value = "false")
class CompressionDisabledTest extends IntegrationSupport {

    @Inject EmbeddedServer server;

    private final HttpClient http = HttpClient.newHttpClient();

    @Test
    void largeResponsesAreIdentityEvenWhenZstdIsAccepted() throws Exception {
        HttpResponse<String> r =
                http.send(
                        HttpRequest.newBuilder(server.getURI().resolve("/__compression/sized/4096"))
                                .header("Accept-Encoding", "zstd")
                                .build(),
                        HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        assertThat(r.statusCode()).isEqualTo(200);
        assertThat(r.headers().firstValue("Content-Encoding")).isEmpty();
        assertThat(r.body()).isEqualTo(CompressionFixtures.payload(4096));
    }

    @Test
    void zstdRequestBodiesAreStillDecoded() throws Exception {
        String key = unique("zstd-disabled");
        String json = "{\"key\":\"" + key + "\",\"value\":\"v\"}";
        HttpResponse<String> r =
                http.send(
                        HttpRequest.newBuilder(server.getURI().resolve("/config"))
                                .header("Content-Type", "application/json")
                                .header("Content-Encoding", "zstd")
                                .POST(
                                        HttpRequest.BodyPublishers.ofByteArray(
                                                Zstd.compress(
                                                        json.getBytes(StandardCharsets.UTF_8), 3)))
                                .build(),
                        HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        assertThat(r.statusCode()).isEqualTo(201);
        assertThat(r.body()).isEqualTo(json);
    }
}
