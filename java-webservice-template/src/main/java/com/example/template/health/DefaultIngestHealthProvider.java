package com.example.template.health;

import com.example.template.dto.health.IngestHealth;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

/**
 * Used until Phase 4 supplies the real {@link StreamIngestHealthMarker} provider.
 * Reports the not-configured ingest transport as "connected" so readiness reflects
 * only the dependency stores while no transport exists.
 */
@Singleton
@Requires(missingBeans = StreamIngestHealthMarker.class)
public class DefaultIngestHealthProvider implements IngestHealthProvider {
    @Override
    public IngestHealth currentHealth() {
        return new IngestHealth("none", "connected", false, null, null, 0, false);
    }
}
