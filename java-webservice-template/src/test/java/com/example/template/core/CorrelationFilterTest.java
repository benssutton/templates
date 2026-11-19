package com.example.template.core;

import static org.assertj.core.api.Assertions.assertThat;

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
        HttpResponse<String> resp =
                client.toBlocking().exchange(HttpRequest.GET("/__corrtest"), String.class);
        String header = resp.getHeaders().get("X-Request-ID");
        assertThat(header).isNotBlank();
        assertThat(resp.body()).isEqualTo(header); // MDC on handler thread == echoed id
    }

    @Test
    void adoptsInboundId() {
        HttpResponse<String> resp =
                client.toBlocking()
                        .exchange(
                                HttpRequest.GET("/__corrtest").header("X-Request-ID", "abc123"),
                                String.class);
        assertThat(resp.getHeaders().get("X-Request-ID")).isEqualTo("abc123");
        assertThat(resp.body()).isEqualTo("abc123");
    }

    /** Only 1-128 chars of [A-Za-z0-9._-] are adopted; anything else is replaced, never echoed. */
    @Test
    void replacesUnsafeOrOversizedInboundIds() {
        for (String bad : new String[] {"has space", "semi;colon", "a".repeat(129), "quote\"s"}) {
            HttpResponse<String> resp =
                    client.toBlocking()
                            .exchange(
                                    HttpRequest.GET("/__corrtest").header("X-Request-ID", bad),
                                    String.class);
            String echoed = resp.getHeaders().get("X-Request-ID");
            assertThat(echoed).as(bad).isNotEqualTo(bad).matches("[0-9a-f-]{36}");
            assertThat(resp.body()).isEqualTo(echoed);
        }
        String longest = "A-z.0_".repeat(21) + "xy";
        HttpResponse<String> ok =
                client.toBlocking()
                        .exchange(
                                HttpRequest.GET("/__corrtest").header("X-Request-ID", longest),
                                String.class);
        assertThat(ok.getHeaders().get("X-Request-ID")).isEqualTo(longest);
    }
}
