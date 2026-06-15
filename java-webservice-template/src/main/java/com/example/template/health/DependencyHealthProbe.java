package com.example.template.health;

import com.example.template.dto.health.ProbeResult;

/**
 * Implemented by each store so HealthService discovers it via
 * {@code List<DependencyHealthProbe>}. Replaces the Python hardcoded
 * _probe(ConfigService/DataService/CacheService) gather: each store added in a
 * later phase self-registers with no HealthService change.
 */
public interface DependencyHealthProbe {
    String name();
    ProbeResult probe();
}
