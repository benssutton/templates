package com.example.template.service;

import com.example.template.config.IngestSettings;
import com.example.template.persistence.streamstore.LsmStore;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;

/** Wires the LSM store from its settings; StreamIngestService is a bean of its own. */
@Factory
public class StreamIngestBeans {

    @Singleton
    LsmStore lsmStore(IngestSettings settings) {
        return new LsmStore(
                settings.lsmFlushRows(), settings.lsmCompactionRuns(), settings.lsmMaxRows());
    }
}
