package com.example.template.dto.health;

import io.micronaut.core.annotation.Nullable;
import io.micronaut.serde.annotation.Serdeable;

/**
 * {@code memoryRssBytes} is the real resident set size; null where the OS does not expose it
 * (non-Linux).
 */
@Serdeable
public record ProcessStats(
        double cpuPercent, @Nullable Long memoryRssBytes, int numThreads, int openFiles) {}
