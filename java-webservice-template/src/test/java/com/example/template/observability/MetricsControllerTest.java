package com.example.template.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import com.example.template.support.IntegrationSupport;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

@MicronautTest
class MetricsControllerTest extends IntegrationSupport {

    @Inject
    @Client("/")
    HttpClient client;

    @Test
    void metricsExposesPrometheusTextWithCustomGauges() {
        String body = client.toBlocking().retrieve(HttpRequest.GET("/metrics"));
        assertThat(body).contains("dependency_up");
        assertThat(body).contains("jvm_memory_used_bytes"); // standard Micrometer binder
        // Totals are counters; ingest outcomes are one counter labelled by outcome.
        assertThat(body).contains("# TYPE ingest_rows_ingested_total counter");
        for (String outcome : new String[] {"accepted", "failed", "rejected"}) {
            assertThat(body).contains("ingest_outcomes_total{outcome=\"" + outcome + "\"}");
        }
        assertThat(body).doesNotContain("ingest_settlements");
        assertThat(body).contains("ingest_connection_state{");
        assertThat(body).contains("state=\"connected\"");
        assertThat(body).contains("# TYPE lsm_evicted_rows_total counter");
        assertThat(body).contains("dependency_up{name=\"postgres\"} 1.0");
    }

    /** /metrics is the one Prometheus endpoint; the built-in /prometheus duplicate is off. */
    @Test
    void builtInPrometheusEndpointIsDisabled() {
        HttpClientResponseException e =
                catchThrowableOfType(
                        HttpClientResponseException.class,
                        () -> client.toBlocking().retrieve(HttpRequest.GET("/prometheus")));
        assertThat(e.code()).isEqualTo(404);
    }
}
