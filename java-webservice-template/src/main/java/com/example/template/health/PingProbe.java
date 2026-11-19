package com.example.template.health;

import com.example.template.dto.health.ProbeResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The one {@link DependencyHealthProbe} implementation: time a cheap round trip to a store and
 * report up, or down with the fixed word "unavailable" (the exception is only logged, since health
 * payloads are public). Stores register one in {@link DependencyProbes}.
 */
public final class PingProbe implements DependencyHealthProbe {

    /** A round trip that throws when the store is unreachable. */
    @FunctionalInterface
    public interface Ping {
        void ping() throws Exception;
    }

    private static final Logger LOG = LoggerFactory.getLogger(PingProbe.class);

    private final String name;
    private final Ping ping;

    public PingProbe(String name, Ping ping) {
        this.name = name;
        this.ping = ping;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public ProbeResult probe() {
        long start = System.nanoTime();
        try {
            ping.ping();
            return ProbeResult.up(name, elapsedMs(start));
        } catch (Exception e) {
            LOG.error("{} health check failed: {}", name, e.toString());
            return ProbeResult.down(name, elapsedMs(start), "unavailable");
        }
    }

    private static double elapsedMs(long startNanos) {
        return Math.round((System.nanoTime() - startNanos) / 1_000_000.0 * 100.0) / 100.0;
    }
}
