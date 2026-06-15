package com.example.template.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * try-with-resources boundary timer; mirrors the Python {@code core.correlation.timed()}.
 * Logs the duration (stamped with the correlation id via MDC) and records it as a
 * Server-Timing sample for the current request.
 */
public final class Timed implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(Timed.class);

    private final String label;
    private final long startNanos;

    private Timed(String label) {
        this.label = label;
        this.startNanos = System.nanoTime();
    }

    public static Timed start(String label) {
        return new Timed(label);
    }

    @Override
    public void close() {
        double ms = (System.nanoTime() - startNanos) / 1_000_000.0;
        LOG.debug("{} {}ms", label, String.format("%.2f", ms));
        BoundarySamples.record(label, ms);
    }
}
