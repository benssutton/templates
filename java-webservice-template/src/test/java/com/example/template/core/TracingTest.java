package com.example.template.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.template.support.IntegrationSupport;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.opentelemetry.api.trace.Span;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

/**
 * Tracing is wired with export off: an inbound W3C traceparent still continues into the handler.
 */
@MicronautTest
class TracingTest extends IntegrationSupport {

    private static final String TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";

    @Controller("/__tracetest")
    @ExecuteOn(TaskExecutors.BLOCKING)
    static class TraceProbe {
        @Get
        String traceId() {
            return Span.current().getSpanContext().getTraceId();
        }
    }

    @Inject
    @Client("/")
    HttpClient client;

    @Test
    void inboundTraceparentIsContinued() {
        String body =
                client.toBlocking()
                        .retrieve(
                                HttpRequest.GET("/__tracetest")
                                        .header(
                                                "traceparent",
                                                "00-" + TRACE_ID + "-00f067aa0ba902b7-01"));
        assertThat(body).isEqualTo(TRACE_ID);
    }

    /**
     * With export off there are no span processors, and since OpenTelemetry SDK 1.68 such a tracer
     * provider hands out no-op tracers: no spans are created, so a request without a traceparent
     * has no trace. Setting OTEL_TRACES_EXPORTER turns span creation on.
     */
    @Test
    void withExportOffRequestsWithoutTraceparentCreateNoSpan() {
        String body = client.toBlocking().retrieve(HttpRequest.GET("/__tracetest"));
        assertThat(body).isEqualTo("0".repeat(32));
    }
}
