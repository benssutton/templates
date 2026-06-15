package com.example.template.core;

import com.example.template.support.IntegrationSupport;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import static org.assertj.core.api.Assertions.assertThat;

@MicronautTest
class CorrelationFilterTest extends IntegrationSupport {

    @Controller("/__corrtest")
    @ExecuteOn(TaskExecutors.BLOCKING)
    static class Probe {
        @Get
        String mdcValue() {
            // Runs on the (virtual) handler thread: the id must have propagated here.
            return String.valueOf(MDC.get("correlationId"));
        }
    }

    @Inject
    @Client("/")
    HttpClient client;

    @Test
    void generatesIdEchoesHeaderAndPopulatesMdc() {
        HttpResponse<String> resp = client.toBlocking().exchange(
            HttpRequest.GET("/__corrtest"), String.class);
        String header = resp.getHeaders().get("X-Request-ID");
        assertThat(header).isNotBlank();
        assertThat(resp.body()).isEqualTo(header); // MDC on handler thread == echoed id
    }

    @Test
    void adoptsInboundId() {
        HttpResponse<String> resp = client.toBlocking().exchange(
            HttpRequest.GET("/__corrtest").header("X-Request-ID", "abc123"), String.class);
        assertThat(resp.getHeaders().get("X-Request-ID")).isEqualTo("abc123");
        assertThat(resp.body()).isEqualTo("abc123");
    }
}
