package com.example.template.service;

import com.example.template.config.IngestSettings;
import com.example.template.ingestion.BatchConsumer;
import com.example.template.persistence.streamstore.LsmStore;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;

/** Wires the LSM store and eagerly starts the ingest service (the lifespan analogue). */
@Factory
public class StreamIngestBeans {

    @Singleton
    LsmStore lsmStore() {
        return new LsmStore();
    }

    @Context
    StreamIngestService streamIngestService(BatchConsumer consumer, LsmStore store, IngestSettings settings) {
        // SIGTERM analogue: non-zero exit so an orchestrator restarts the process.
        StreamIngestService svc = new StreamIngestService(consumer, store, settings, () -> System.exit(3));
        svc.start();
        return svc;
    }
}
