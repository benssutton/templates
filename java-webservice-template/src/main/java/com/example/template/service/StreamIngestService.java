package com.example.template.service;

import com.example.template.config.IngestSettings;
import com.example.template.core.Timed;
import com.example.template.dto.health.IngestHealth;
import com.example.template.health.IngestHealthProvider;
import com.example.template.health.StreamIngestHealthMarker;
import com.example.template.ingestion.BatchConsumer;
import com.example.template.ingestion.ConnectionState;
import com.example.template.persistence.streamstore.LsmRow;
import com.example.template.persistence.streamstore.LsmStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Owns the ingest thread (consume loop with jittered backoff + consecutive-failure
 * shutdown) and a disconnect watchdog. Mirrors Python services/stream_ingest.py.
 * Implements the ingest-health seam so /health reports real transport state.
 */
public class StreamIngestService implements IngestHealthProvider, StreamIngestHealthMarker, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(StreamIngestService.class);
    private static final double BASE_DELAY = 1.0;
    private static final double MAX_DELAY = 60.0;
    private static final int MAX_FAILURES = 5;
    private static final long JOIN_TIMEOUT_MS = 10_000;

    private final BatchConsumer consumer;
    private final LsmStore store;
    private final IngestSettings settings;
    private final Runnable shutdownHook;

    private Thread thread;
    private ScheduledExecutorService watchdog;
    private volatile Instant lastBatchAt;
    private final Instant startedAt = Instant.now();
    private final AtomicLong rowsTotal = new AtomicLong();
    private volatile Instant disconnectedSince;

    public StreamIngestService(BatchConsumer consumer, LsmStore store, IngestSettings settings, Runnable shutdownHook) {
        this.consumer = consumer;
        this.store = store;
        this.settings = settings;
        this.shutdownHook = shutdownHook;
    }

    public void start() {
        thread = new Thread(this::ingestLoop, "ingest-loop");
        thread.setDaemon(true);
        thread.start();
        if (settings.getMaxDisconnectSeconds() > 0) {
            watchdog = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "ingest-watchdog");
                t.setDaemon(true);
                return t;
            });
            long poll = Math.max(1, Math.min(settings.getMaxDisconnectSeconds() / 2, 5));
            watchdog.scheduleAtFixedRate(this::checkDisconnect, poll, poll, TimeUnit.SECONDS);
        }
    }

    private void record(List<LsmRow> batch) {
        store.ingest(batch);
        lastBatchAt = Instant.now();
        rowsTotal.addAndGet(batch.size());
    }

    private void ingestLoop() {
        int consecutiveFailures = 0;
        double delay = BASE_DELAY;
        boolean shutdownOnFailure = settings.getMaxDisconnectSeconds() > 0;
        while (true) {
            try {
                consumer.consume(batch -> {
                    String token = MDC.get("correlationId");
                    MDC.put("correlationId", UUID.randomUUID().toString());
                    try {
                        record(batch);
                    } catch (Exception e) {
                        LOG.error("ingest failed; skipping batch", e);
                    } finally {
                        if (token == null) {
                            MDC.remove("correlationId");
                        } else {
                            MDC.put("correlationId", token);
                        }
                    }
                });
                return; // clean end — consumer closed
            } catch (Exception e) {
                consecutiveFailures++;
                LOG.error("consumer failed (failure {}/{})", consecutiveFailures, MAX_FAILURES, e);
                if (shutdownOnFailure && consecutiveFailures >= MAX_FAILURES) {
                    LOG.error("ingest: {} consecutive failures; requesting shutdown", consecutiveFailures);
                    shutdownHook.run();
                    return;
                }
                double jitter = delay * 0.25 * ThreadLocalRandom.current().nextDouble();
                try {
                    Thread.sleep((long) ((delay + jitter) * 1000));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
                delay = Math.min(delay * 2, MAX_DELAY);
            }
        }
    }

    private void checkDisconnect() {
        if (consumer.connectionState() == ConnectionState.CONNECTED) {
            disconnectedSince = null;
            return;
        }
        Instant now = Instant.now();
        if (disconnectedSince == null) {
            disconnectedSince = now;
        } else if (now.getEpochSecond() - disconnectedSince.getEpochSecond() >= settings.getMaxDisconnectSeconds()) {
            LOG.error("ingest transport not connected for {}s; requesting shutdown", settings.getMaxDisconnectSeconds());
            shutdownHook.run();
        }
    }

    /** HTTP ingest path: synchronous write with a Server-Timing boundary. */
    public void ingestBatch(List<LsmRow> batch) {
        if (!batch.isEmpty()) {
            try (Timed t = Timed.start("ingest.lsm_write")) {
                record(batch);
            }
        }
    }

    @Override
    public IngestHealth currentHealth() {
        ConnectionState state = consumer.connectionState();
        Instant last = lastBatchAt;
        Double secondsSince = last != null ? (Instant.now().toEpochMilli() - last.toEpochMilli()) / 1000.0 : null;
        double threshold = settings.getStalenessThresholdSeconds();
        boolean stale = false;
        if (threshold > 0) {
            double elapsed = secondsSince != null ? secondsSince
                : (Instant.now().toEpochMilli() - startedAt.toEpochMilli()) / 1000.0;
            stale = elapsed > threshold;
        }
        return new IngestHealth(settings.getTransport(), state.value(),
            thread != null && thread.isAlive(), last,
            secondsSince != null ? Math.round(secondsSince * 1000.0) / 1000.0 : null,
            rowsTotal.get(), stale);
    }

    @Override
    public void close() {
        if (watchdog != null) {
            watchdog.shutdownNow();
        }
        consumer.close();
        if (thread != null) {
            try {
                thread.join(JOIN_TIMEOUT_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (thread.isAlive()) {
                LOG.error("ingest thread did not stop within {}ms; abandoning", JOIN_TIMEOUT_MS);
            }
        }
    }
}
