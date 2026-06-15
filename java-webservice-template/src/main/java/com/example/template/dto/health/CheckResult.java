package com.example.template.dto.health;

import io.micronaut.serde.annotation.Serdeable;

import java.time.Instant;

@Serdeable
public record CheckResult(String name, String status, Double latencyMs, String transport,
                          String connectionState, Boolean threadAlive, Instant lastBatchAt,
                          Double secondsSinceLastBatch, String error) {}
