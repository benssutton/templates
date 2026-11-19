package com.example.template.service;

import com.example.template.config.IngestSettings;
import com.example.template.core.CorrelationId;
import com.example.template.core.Timed;
import com.example.template.dto.health.IngestHealth;
import com.example.template.health.IngestHealthProvider;
import com.example.template.ingestion.ArrowDecoder;
import com.example.template.ingestion.BatchConsumer;
import com.example.template.ingestion.ConnectionState;
import com.example.template.ingestion.Delivery;
import com.example.template.ingestion.MalformedBatchException;
import com.example.template.persistence.streamstore.LsmRow;
import com.example.template.persistence.streamstore.LsmStore;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Owns the ingest thread and decides how each transport delivery is settled.
 *
 * <ul>
 *   <li>Consume loop: a transport failure is retried with jittered exponential backoff ({@code
 *       retry-base-delay-seconds} doubling to {@code retry-max-delay-seconds}); {@code
 *       max-consecutive-failures} failures in a row without a successful bind request shutdown.
 *   <li>Disconnect watchdog: the transport staying disconnected for {@code max-disconnect-seconds}
 *       requests shutdown.
 *   <li>Settlement backoff: after a FAIL, the same backoff before taking the next delivery, since
 *       the broker redelivers at once.
 * </ul>
 *
 * Every backoff wait is cut short by {@link #close()}.
 */
public class StreamIngestService implements IngestHealthProvider, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(StreamIngestService.class);
    private static final long JOIN_TIMEOUT_MS = 10_000;

    /** How a delivery was settled; the lowercase name is the metric tag value. */
    public enum Outcome {
        ACCEPT,
        FAIL,
        REJECT;

        public String tag() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private final BatchConsumer consumer;
    private final LsmStore store;
    private final ArrowDecoder decoder;
    private final IngestSettings settings;
    private final Runnable shutdownHook;

    /** Released first thing in close(): wakes any backoff wait so shutdown is prompt. */
    private final CountDownLatch stop = new CountDownLatch(1);

    private volatile Thread thread;
    private volatile ScheduledExecutorService watchdog;
    private volatile Instant lastBatchAt;
    private final Instant startedAt = Instant.now();
    private final AtomicLong rowsTotal = new AtomicLong();
    private final Map<Outcome, AtomicLong> settlements = new EnumMap<>(Outcome.class);
    private volatile Instant disconnectedSince;

    /** The watchdog requests shutdown at most once. */
    private final AtomicBoolean watchdogFired = new AtomicBoolean();

    // Backoff state, touched only by the ingest thread (onBound and the sink both run
    // on it, inside consume()). A successful bind, or a delivery, proves the transport
    // works, so it resets the transport backoff and the consecutive-failure count.
    private int consecutiveFailures;
    private double delay;
    // Settlement backoff, separate from the transport backoff: fail() makes the broker
    // redeliver immediately, so a persistent store error would otherwise spin.
    private double settleDelay;

    public StreamIngestService(
            BatchConsumer consumer,
            LsmStore store,
            ArrowDecoder decoder,
            IngestSettings settings,
            Runnable shutdownHook) {
        this.consumer = consumer;
        this.store = store;
        this.decoder = decoder;
        this.settings = settings;
        this.shutdownHook = shutdownHook;
        this.delay = settings.retryBaseDelaySeconds();
        this.settleDelay = settings.retryBaseDelaySeconds();
        for (Outcome o : Outcome.values()) {
            settlements.put(o, new AtomicLong());
        }
    }

    public void start() {
        thread = new Thread(this::ingestLoop, "ingest-loop");
        thread.setDaemon(true);
        thread.start();
        if (settings.maxDisconnectSeconds() > 0) {
            watchdog =
                    Executors.newSingleThreadScheduledExecutor(
                            r -> {
                                Thread t = new Thread(r, "ingest-watchdog");
                                t.setDaemon(true);
                                return t;
                            });
            long poll = Math.max(1, Math.min(settings.maxDisconnectSeconds() / 2, 5));
            watchdog.scheduleAtFixedRate(this::checkDisconnect, poll, poll, TimeUnit.SECONDS);
        }
    }

    private void record(List<LsmRow> batch) {
        store.ingest(batch);
        lastBatchAt = Instant.now();
        rowsTotal.addAndGet(batch.size());
    }

    /**
     * Size, then decode, then write; never settles, so a raising callback can't cause a second
     * settlement. The size check comes first so an oversized message costs no decoding memory.
     */
    private Outcome decide(Delivery delivery) {
        byte[] payload = delivery.payload();
        if (payload.length > settings.maxBatchBytes()) {
            LOG.error(
                    "rejecting ingest message: {} bytes exceeds max-batch-bytes ({})",
                    payload.length,
                    settings.maxBatchBytes());
            return Outcome.REJECT;
        }
        List<LsmRow> rows;
        try {
            rows = decoder.decodeAll(payload);
        } catch (MalformedBatchException e) {
            LOG.warn("rejecting unstorable ingest message: {}", e.getMessage());
            return Outcome.REJECT;
        } catch (RuntimeException e) {
            // e.g. the shared decoder allocator is momentarily full: worth a redelivery.
            LOG.error("decoding failed; message will be redelivered", e);
            return Outcome.FAIL;
        }
        try {
            record(rows);
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
        settlements.get(outcome).incrementAndGet();
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
        String previous = MDC.get(CorrelationId.MDC_KEY);
        MDC.put(CorrelationId.MDC_KEY, CorrelationId.adoptOrGenerate(delivery.correlationId()));
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
            if (previous == null) {
                MDC.remove(CorrelationId.MDC_KEY);
            } else {
                MDC.put(CorrelationId.MDC_KEY, previous);
            }
        }
        if (outcome == Outcome.FAIL) {
            if (backoff(settleDelay)) {
                return; // shutting down; consume() sees the consumer closed
            }
            settleDelay = Math.min(settleDelay * 2, settings.retryMaxDelaySeconds());
        } else if (outcome != null) {
            settleDelay = settings.retryBaseDelaySeconds();
        }
    }

    /** Ingest thread only: the transport is proven healthy (bound, or delivering). */
    private void resetTransportBackoff() {
        consecutiveFailures = 0;
        delay = settings.retryBaseDelaySeconds();
    }

    private void ingestLoop() {
        int maxFailures = settings.maxConsecutiveFailures();
        while (!stopping()) {
            try {
                consumer.consume(this::onDelivery, this::resetTransportBackoff);
                return; // clean end — consumer closed
            } catch (Exception e) {
                if (stopping()) {
                    return; // close() tore the transport down under us
                }
                consecutiveFailures++;
                LOG.error("consumer failed (failure {} in a row)", consecutiveFailures, e);
                if (maxFailures > 0 && consecutiveFailures >= maxFailures) {
                    LOG.error(
                            "ingest: {} consecutive failures without a successful bind; requesting shutdown",
                            consecutiveFailures);
                    shutdownHook.run();
                    return;
                }
                if (backoff(delay)) {
                    return; // shutting down
                }
                delay = Math.min(delay * 2, settings.retryMaxDelaySeconds());
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
        } else if (Duration.between(disconnectedSince, now).toMillis()
                        >= settings.maxDisconnectSeconds() * 1000L
                && watchdogFired.compareAndSet(false, true)) {
            LOG.error(
                    "ingest transport not connected for {}s; requesting shutdown",
                    settings.maxDisconnectSeconds());
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

    public long rowsIngestedTotal() {
        return rowsTotal.get();
    }

    public long settlementCount(Outcome outcome) {
        return settlements.get(outcome).get();
    }

    @Override
    public IngestHealth currentHealth() {
        ConnectionState state = consumer.connectionState();
        Instant last = lastBatchAt;
        Double secondsSince =
                last != null ? (Instant.now().toEpochMilli() - last.toEpochMilli()) / 1000.0 : null;
        double threshold = settings.stalenessThresholdSeconds();
        boolean stale = false;
        if (threshold > 0) {
            double elapsed =
                    secondsSince != null
                            ? secondsSince
                            : (Instant.now().toEpochMilli() - startedAt.toEpochMilli()) / 1000.0;
            stale = elapsed > threshold;
        }
        return new IngestHealth(
                settings.transport(),
                state,
                thread != null && thread.isAlive(),
                last,
                secondsSince != null ? Math.round(secondsSince * 1000.0) / 1000.0 : null,
                rowsTotal.get(),
                store.evictedRows(),
                stale);
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
