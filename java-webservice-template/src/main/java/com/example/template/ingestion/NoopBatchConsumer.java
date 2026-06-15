package com.example.template.ingestion;

import com.example.template.persistence.streamstore.LsmRow;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

import java.util.List;
import java.util.function.Consumer;

/**
 * A consumer that connects to nothing and idles until closed. Selected when
 * {@code template.ingest.transport=noop} — used by tests and any deployment with
 * no real transport, so the ingest service can run without a Flight/Solace endpoint.
 */
@Singleton
@Requires(property = "template.ingest.transport", value = "noop")
public class NoopBatchConsumer implements BatchConsumer {

    private final Object lock = new Object();
    private volatile boolean closed = false;

    @Override
    public void consume(Consumer<List<LsmRow>> sink) throws InterruptedException {
        synchronized (lock) {
            while (!closed) {
                lock.wait();
            }
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            closed = true;
            lock.notifyAll();
        }
    }

    @Override
    public ConnectionState connectionState() {
        return closed ? ConnectionState.DOWN : ConnectionState.CONNECTED;
    }
}
