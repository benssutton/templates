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
import org.junit.jupiter.api.Test;

@MicronautTest
@Property(name = "template.compression.require-zstd", value = "true")
@Property(name = "template.auth.enabled", value = "true")
@Property(
        name = "template.auth.jwt-secret",
        value = "strict-mode-auth-test-secret-0123456789abcdef0123456789")
class CompressionStrictAuthTest extends IntegrationSupport {

    @Inject EmbeddedServer server;

    @Test
    void strictModeAnswers406BeforeAuthentication() throws Exception {
        HttpRequest request =
                HttpRequest.newBuilder(server.getURI().resolve("/config"))
                        .header("Accept-Encoding", "gzip")
                        .GET()
                        .build();
        HttpResponse<String> r =
                HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(r.statusCode()).isEqualTo(406);
    }
}
