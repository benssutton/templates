package com.example.template.service;

import com.example.template.config.IngestSettings;
import com.example.template.core.Timed;
import com.example.template.dto.health.IngestHealth;
import com.example.template.health.IngestHealthProvider;
import com.example.template.health.StreamIngestHealthMarker;
import com.example.template.ingestion.BatchConsumer;
import com.example.template.ingestion.ConnectionState;
import com.example.template.ingestion.Delivery;
import com.example.template.persistence.streamstore.LsmRow;
import com.example.template.persistence.streamstore.LsmStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Owns the ingest thread (consume loop with jittered backoff + shutdown after
 * consecutive failures without a successful bind), a settlement backoff after each failed delivery, and a disconnect
 * watchdog. Every backoff wait is cut short by {@link #close()}. Mirrors Python
 * services/stream_ingest.py.
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
    /** Released first thing in close(): wakes any backoff wait so shutdown is prompt. */
    private final CountDownLatch stop = new CountDownLatch(1);

    private volatile Thread thread;
    private volatile ScheduledExecutorService watchdog;
    private volatile Instant lastBatchAt;
    private final Instant startedAt = Instant.now();
    private final AtomicLong rowsTotal = new AtomicLong();
    private volatile Instant disconnectedSince;
    /** The watchdog requests shutdown at most once (Python's watchdog returns after it). */
    private final AtomicBoolean watchdogFired = new AtomicBoolean();

    // Backoff state, touched only by the ingest thread (onBound and the sink both run
    // on it, inside consume()). A successful bind, or a delivery, proves the transport
    // works, so it resets the transport backoff (as the Python loop does): the
    // shutdown fires only on MAX_FAILURES consecutive failures without a bind.
    private int consecutiveFailures;
    private double delay = BASE_DELAY;
    // Settlement backoff, separate from the transport backoff (`delay`): fail() makes
    // the broker redeliver immediately (CREATE_ON_START queues have unlimited
    // max-redelivery and no DMQ), so a persistent store error would otherwise spin.
    // This only prevents a hot loop; production queues should set max-redelivery
    // and a DMQ (see Delivery).
    private double settleDelay = BASE_DELAY;

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

    private enum Outcome { ACCEPT, FAIL, REJECT }

    /** Write the rows and decide the outcome. Never settles, so a raising callback can't cause a second settlement. */
    private Outcome decide(Delivery delivery) {
        if (delivery.payloadBytes() > settings.getMaxBatchBytes()) {
            LOG.error("rejecting ingest message: {} bytes exceeds max-batch-bytes ({})",
                delivery.payloadBytes(), settings.getMaxBatchBytes());
            return Outcome.REJECT;
        }
        try {
            record(delivery.rows());
        } catch (RuntimeException e) {
            LOG.error("ingest failed; message will be redelivered", e);
            return Outcome.FAIL;
        }
        return Outcome.ACCEPT;
    }

    /** Settle exactly once. A raising callback propagates to the sink's handler. */
    private Outcome settle(Delivery delivery) {
        Outcome outcome = decide(delivery);
        switch (outcome) {
            case ACCEPT -> delivery.accept();
            case FAIL -> delivery.fail();
            case REJECT -> delivery.reject();
        }
        return outcome;
    }

    /** Jittered wait (delay..1.25*delay seconds). True when close() interrupted it. */
    private boolean backoff(double delaySeconds) {
        double jitter = delaySeconds * 0.25 * ThreadLocalRandom.current().nextDouble();
        try {
            return stop.await((long) ((delaySeconds + jitter) * 1000), TimeUnit.MILLISECONDS);
        } catch (InterruptedException ie) {
            // Nothing in this service interrupts the ingest thread (close() uses the
            // latch), so an interrupt is foreign: keep the flag set and stop ingesting.
            Thread.currentThread().interrupt();
            LOG.warn("ingest backoff interrupted; stopping the ingest loop");
            return true;
        }
    }

    private boolean stopping() {
        return stop.getCount() == 0;
    }

    private void onDelivery(Delivery delivery) {
        if (stopping()) {
            return; // left unsettled: the broker redelivers it on the next bind
        }
        resetTransportBackoff();
        Outcome outcome = null;
        String token = MDC.get("correlationId");
        MDC.put("correlationId", UUID.randomUUID().toString());
        try {
            outcome = settle(delivery);
        } catch (RuntimeException e) {
            // settle() calls into the SDK; if the session just dropped, the
            // broker still holds the unsettled message and will redeliver it.
            if (stopping()) {
                LOG.warn("settlement failed during shutdown; broker will redeliver");
            } else {
                LOG.error("settlement failed; broker will redeliver", e);
            }
        } finally {
            if (token == null) {
                MDC.remove("correlationId");
            } else {
                MDC.put("correlationId", token);
            }
        }
        if (outcome == Outcome.FAIL) {
            if (backoff(settleDelay)) {
                return; // shutting down; consume() sees the consumer closed
            }
            settleDelay = Math.min(settleDelay * 2, MAX_DELAY);
        } else if (outcome != null) {
            settleDelay = BASE_DELAY;
        }
    }

    /** Ingest thread only: the transport is proven healthy (bound, or delivering). */
    private void resetTransportBackoff() {
        consecutiveFailures = 0;
        delay = BASE_DELAY;
    }

    private void ingestLoop() {
        boolean shutdownOnFailure = settings.getMaxDisconnectSeconds() > 0;
        while (!stopping()) {
            try {
                consumer.consume(this::onDelivery, this::resetTransportBackoff);
                return; // clean end — consumer closed
            } catch (Exception e) {
                if (stopping()) {
                    return; // close() tore the transport down under us
                }
                consecutiveFailures++;
                LOG.error("consumer failed (failure {}/{})", consecutiveFailures, MAX_FAILURES, e);
                if (shutdownOnFailure && consecutiveFailures >= MAX_FAILURES) {
                    LOG.error("ingest: {} consecutive failures without a successful bind; requesting shutdown", consecutiveFailures);
                    shutdownHook.run();
                    return;
                }
                if (backoff(delay)) {
                    return; // shutting down
                }
                delay = Math.min(delay * 2, MAX_DELAY);
            }
        }
    }

    private void checkDisconnect() {
        if (stopping() || watchdogFired.get()) {
            return;
        }
        if (consumer.connectionState() == ConnectionState.CONNECTED) {
            disconnectedSince = null;
            return;
        }
        Instant now = Instant.now();
        if (disconnectedSince == null) {
            disconnectedSince = now;
        } else if (Duration.between(disconnectedSince, now).toMillis() >= settings.getMaxDisconnectSeconds() * 1000L
            && watchdogFired.compareAndSet(false, true)) {
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
        stop.countDown();
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
