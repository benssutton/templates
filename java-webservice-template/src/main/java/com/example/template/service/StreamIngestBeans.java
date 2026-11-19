package com.example.template.service;

import com.example.template.config.IngestSettings;
import com.example.template.ingestion.ArrowDecoder;
import com.example.template.ingestion.BatchConsumer;
import com.example.template.persistence.streamstore.LsmStore;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;

/** Wires the LSM store and eagerly starts the ingest service. */
@Factory
public class StreamIngestBeans {

    @Singleton
    LsmStore lsmStore(IngestSettings settings) {
        return new LsmStore(settings.lsmMaxRows());
    }

    /**
     * {@code preDestroy} is the shutdown half of the lifecycle: Micronaut does not infer {@code
     * AutoCloseable.close()} for factory beans, so without it context shutdown would leave the
     * ingest thread running and the transport session open. The service owns its consumer and
     * closes it; the consumer beans have no destroy hook of their own.
     */
    @Context
    @Bean(preDestroy = "close")
    StreamIngestService streamIngestService(
            BatchConsumer consumer, LsmStore store, ArrowDecoder decoder, IngestSettings settings) {
        // Non-zero exit so an orchestrator restarts the process. Exit from a separate
        // thread: System.exit blocks its caller while shutdown hooks run, and Micronaut's
        // hook closes this service, which joins the ingest thread. Calling exit on the
        // ingest/watchdog thread would stall that join for its full timeout and log a
        // false "did not stop" error.
        Runnable exit = () -> new Thread(() -> System.exit(3), "ingest-exit").start();
        StreamIngestService svc = new StreamIngestService(consumer, store, decoder, settings, exit);
        svc.start();
        return svc;
    }
}
