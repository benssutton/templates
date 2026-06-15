package com.example.template.dto.health;

import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record ProbeResult(String name, String status, double latencyMs, String error) {
    public static ProbeResult up(String name, double latencyMs) {
        return new ProbeResult(name, "up", latencyMs, null);
    }

    public static ProbeResult down(String name, double latencyMs, String error) {
        return new ProbeResult(name, "down", latencyMs, error);
    }
}
