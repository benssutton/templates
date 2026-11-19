package com.example.template.auth;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.template.support.IntegrationSupport;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

/** Default: auth off, every route open, a token is simply ignored. */
@MicronautTest
class AuthDisabledTest extends IntegrationSupport {

    @Inject
    @Client("/")
    HttpClient client;

    @Test
    void routesAreOpenByDefault() throws Exception {
        assertThat(client.toBlocking().exchange(HttpRequest.GET("/config")).code()).isEqualTo(200);
        assertThat(
                        client.toBlocking()
                                .exchange(
                                        HttpRequest.GET("/config")
                                                .bearerAuth(AuthTest.validToken()))
                                .code())
                .isEqualTo(200);
    }
}
