package com.example.template.core;

import com.example.template.support.PostgresTestSupport;
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

import static org.assertj.core.api.Assertions.assertThat;

@MicronautTest
class ServerTimingFilterTest extends PostgresTestSupport {

    @Controller("/__sttest")
    @ExecuteOn(TaskExecutors.BLOCKING)
    static class Probe {
        @Get
        String work() throws InterruptedException {
            try (Timed t = Timed.start("db.query")) {
                Thread.sleep(5);
            }
            return "ok";
        }
    }

    @Inject
    @Client("/")
    HttpClient client;

    @Test
    void emitsServerTimingHeaderWithBoundaryAndTotal() {
        HttpResponse<String> resp = client.toBlocking().exchange(HttpRequest.GET("/__sttest"), String.class);
        String st = resp.getHeaders().get("Server-Timing");
        assertThat(st).contains("db_query;dur=").contains("total;dur=");
    }
}
