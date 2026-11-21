package com.example.template.health;

import com.example.template.dto.health.ProbeResult;

/**
 * Implemented by each store so HealthService discovers it via {@code List<DependencyHealthProbe>}.
 * A new store registers itself by providing a probe; HealthService needs no change.
 */
public interface DependencyHealthProbe {
    String name();

    ProbeResult probe();
}
