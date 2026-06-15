package com.example.template.observability;

import com.example.template.dto.health.IngestHealth;
import com.example.template.dto.health.ProbeResult;
import com.example.template.service.HealthService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import jakarta.inject.Singleton;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Registers and refreshes custom gauges from the health snapshot. Mirrors the
 * Python MetricsService.refresh() pull model; standard JVM/process metrics come
 * from Micrometer's built-in binders (configured in application.yml).
 */
@Singleton
public class MetricsBinder {

    private final MeterRegistry registry;
    private final HealthService health;
    private final ConcurrentHashMap<String, MutableDouble> values = new ConcurrentHashMap<>();

    public MetricsBinder(MeterRegistry registry, HealthService health) {
        this.registry = registry;
        this.health = health;
    }

    private MutableDouble gauge(String name, Tags tags, String key) {
        return values.computeIfAbsent(key, k -> {
            MutableDouble v = new MutableDouble();
            registry.gauge(name, tags, v, MutableDouble::get);
            return v;
        });
    }

    public void refresh() {
        var status = health.detailedStatus();
        for (ProbeResult dep : status.dependencies()) {
            gauge("dependency_up", Tags.of("name", dep.name()), "up:" + dep.name())
                .set(dep.status().equals("up") ? 1.0 : 0.0);
            gauge("dependency_check_latency_seconds", Tags.of("name", dep.name()), "lat:" + dep.name())
                .set(dep.latencyMs() / 1000.0);
        }
        IngestHealth ingest = status.ingest();
        gauge("ingest_rows_ingested", Tags.empty(), "ingest_rows").set(ingest.rowsIngestedTotal());
        Double secs = ingest.secondsSinceLastBatch();
        gauge("ingest_seconds_since_last_batch", Tags.empty(), "ingest_secs")
            .set(secs == null ? Double.NaN : secs);
    }
}
