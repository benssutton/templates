package com.example.template.health;

/**
 * Marker implemented by the real StreamIngestService (Phase 4). Its presence
 * disables {@link DefaultIngestHealthProvider} so real ingest health is used.
 */
public interface StreamIngestHealthMarker {}
