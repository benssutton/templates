package com.example.template.dto.health;

import com.example.template.ingestion.ConnectionState;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.serde.annotation.Serdeable;
import java.time.Instant;

/** {@code rowsEvictedTotal}: rows dropped by the LSM store's retention cap since start. */
@Serdeable
public record IngestHealth(
        String transport,
        ConnectionState connectionState,
        boolean threadAlive,
        @Nullable Instant lastBatchAt,
        @Nullable Double secondsSinceLastBatch,
        long rowsIngestedTotal,
        long rowsEvictedTotal,
        boolean stale) {}
