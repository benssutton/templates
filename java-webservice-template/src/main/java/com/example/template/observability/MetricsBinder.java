package com.example.template.observability;

import com.example.template.dto.health.ProbeResult;
import com.example.template.ingestion.ConnectionState;
import com.example.template.persistence.streamstore.LsmStore;
import com.example.template.service.HealthService;
import com.example.template.service.StreamIngestService;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micronaut.context.annotation.Context;

/**
 * Registers the template's custom meters once, at startup. Each reads its value when scraped:
 * dependency gauges read {@link HealthService#dependencies()}, which reuses one probe round for
 * {@code health-cache-ttl-seconds}, so a scrape does not ping every store once per gauge. Totals
 * are counters (exported with {@code _total}). Standard JVM/process metrics come from Micrometer's
 * built-in binders (application.yml).
 */
@Context
public class MetricsBinder {

    public MetricsBinder(
            MeterRegistry registry,
            HealthService health,
            StreamIngestService ingest,
            LsmStore store) {
        for (String name : health.dependencyNames()) {
            Gauge.builder("dependency_up", health, h -> probe(h, name).isUp() ? 1.0 : 0.0)
                    .tag("name", name)
                    .description("1 when the dependency's last health probe succeeded")
                    .register(registry);
            Gauge.builder(
                            "dependency_check_latency_seconds",
                            health,
                            h -> probe(h, name).latencyMs() / 1000.0)
                    .tag("name", name)
                    .register(registry);
        }
        FunctionCounter.builder(
                        "ingest_rows_ingested", ingest, StreamIngestService::rowsIngestedTotal)
                .description("Rows written to the LSM store (HTTP and transport)")
                .register(registry);
        for (StreamIngestService.Outcome outcome : StreamIngestService.Outcome.values()) {
            FunctionCounter.builder("ingest_outcomes", ingest, s -> s.outcomeCount(outcome))
                    .tag("outcome", outcome.tag())
                    .description("Ingest HTTP requests and transport deliveries, by outcome")
                    .register(registry);
        }
        FunctionCounter.builder("lsm_evicted_rows", store, LsmStore::evictedRows)
                .description("Rows dropped by the LSM store's retention cap")
                .register(registry);
        String transport = ingest.currentHealth().transport();
        for (ConnectionState state : ConnectionState.values()) {
            Gauge.builder(
                            "ingest_connection_state",
                            ingest,
                            s -> s.currentHealth().connectionState() == state ? 1.0 : 0.0)
                    .tag("transport", transport)
                    .tag("state", state.value())
                    .description("1 for the transport's current connection state")
                    .register(registry);
        }
        Gauge.builder(
                        "ingest_seconds_since_last_batch",
                        ingest,
                        s -> {
                            Double secs = s.currentHealth().secondsSinceLastBatch();
                            return secs == null ? Double.NaN : secs;
                        })
                .register(registry);
    }

    private static ProbeResult probe(HealthService health, String name) {
        return health.dependencies().stream()
                .filter(p -> p.name().equals(name))
                .findFirst()
                .orElseThrow();
    }
}
