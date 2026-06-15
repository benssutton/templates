package com.example.template.controller;

import com.example.template.dto.RootInfo;
import com.example.template.support.IntegrationSupport;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@MicronautTest
class RootControllerTest extends IntegrationSupport {

    @Inject
    @Client("/")
    HttpClient client;

    @Test
    void rootReturnsServiceInfo() {
        RootInfo info = client.toBlocking().retrieve(HttpRequest.GET("/"), RootInfo.class);
        assertThat(info.title()).isNotBlank();
        assertThat(info.docs()).isEqualTo("/swagger-ui");
        assertThat(info.mcp()).isEqualTo("/mcp");
    }
}
