package com.example.template.dto.health;

import io.micronaut.serde.annotation.Serdeable;

import java.time.Instant;

@Serdeable
public record IngestHealth(String transport, String connectionState, boolean threadAlive,
                           Instant lastBatchAt, Double secondsSinceLastBatch,
                           long rowsIngestedTotal, boolean stale) {}
