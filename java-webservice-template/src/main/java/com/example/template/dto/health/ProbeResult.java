package com.example.template.dto.health;

import io.micronaut.core.annotation.Nullable;
import io.micronaut.serde.annotation.Serdeable;

/**
 * {@code error} is a fixed word ("unavailable"), never the exception: health payloads are public.
 */
@Serdeable
public record ProbeResult(
        String name, ProbeStatus status, double latencyMs, @Nullable String error) {
    public static ProbeResult up(String name, double latencyMs) {
        return new ProbeResult(name, ProbeStatus.UP, latencyMs, null);
    }

    public static ProbeResult down(String name, double latencyMs, String error) {
        return new ProbeResult(name, ProbeStatus.DOWN, latencyMs, error);
    }

    public boolean isUp() {
        return status == ProbeStatus.UP;
    }
}
