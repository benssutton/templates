package com.example.template.ingestion;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;

import java.util.function.Consumer;

/**
 * A consumer that connects to nothing and idles until closed. Selected when
 * {@code template.ingest.transport=noop} — used by tests and any deployment with
 * no real transport, so the ingest service can run without a Solace endpoint.
 */
@Singleton
@Requires(property = "template.ingest.transport", value = "noop")
public class NoopBatchConsumer implements BatchConsumer {

    private final Object lock = new Object();
    private volatile boolean closed = false;

    @Override
    public void consume(Consumer<Delivery> sink) throws InterruptedException {
        consume(sink, () -> { });
    }

    /**
     * Signals bound at once: there is nothing to connect to, so it is "ready" as
     * soon as it idles, which matches connectionState() reporting CONNECTED. It
     * only ever ends cleanly, so the signal can't hide a failure.
     */
    @Override
    public void consume(Consumer<Delivery> sink, Runnable onBound) throws InterruptedException {
        if (!closed) {
            onBound.run();
        }
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
