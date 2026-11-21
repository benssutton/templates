package com.example.template.dto.health;

import com.example.template.ingestion.ConnectionState;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.serde.annotation.Serdeable;
import java.time.Instant;

/** One readiness check: a dependency (latency, error) or the ingest transport (the rest). */
@Serdeable
public record CheckResult(
        String name,
        ProbeStatus status,
        @Nullable Double latencyMs,
        @Nullable String transport,
        @Nullable ConnectionState connectionState,
        @Nullable Boolean threadAlive,
        @Nullable Instant lastBatchAt,
        @Nullable Double secondsSinceLastBatch,
        @Nullable String error) {

    public static CheckResult dependency(ProbeResult p) {
        return new CheckResult(
                p.name(), p.status(), p.latencyMs(), null, null, null, null, null, p.error());
    }

    public static CheckResult ingest(IngestHealth h, ProbeStatus status) {
        return new CheckResult(
                "ingest",
                status,
                null,
                h.transport(),
                h.connectionState(),
                h.threadAlive(),
                h.lastBatchAt(),
                h.secondsSinceLastBatch(),
                null);
    }
}
