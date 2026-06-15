package com.example.template.core;

import com.example.template.support.IntegrationSupport;
import io.micronaut.context.annotation.Property;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

@MicronautTest
@Property(name = "micronaut.server.max-request-size", value = "100")
class BodySizeTest extends IntegrationSupport {

    @Controller("/__bodytest")
    static class Sink {
        @Post(consumes = MediaType.TEXT_PLAIN)
        String accept(@Body String body) {
            return "len=" + body.length();
        }
    }

    @Inject
    @Client("/")
    HttpClient client;

    @Test
    void oversizedBodyRejectedWith413() {
        String big = "x".repeat(500);
        HttpClientResponseException ex = catchThrowableOfType(
            () -> client.toBlocking().exchange(
                HttpRequest.POST("/__bodytest", big).contentType(MediaType.TEXT_PLAIN)),
            HttpClientResponseException.class);
        assertThat((Object) ex.getStatus()).isEqualTo(HttpStatus.REQUEST_ENTITY_TOO_LARGE);
    }
}
