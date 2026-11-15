package com.example.template.service;

import com.example.template.config.IngestSettings;
import com.example.template.ingestion.BatchConsumer;
import com.example.template.ingestion.ConnectionState;
import com.example.template.ingestion.Delivery;
import com.example.template.persistence.streamstore.LsmRow;
import com.example.template.persistence.streamstore.LsmStore;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** StreamIngestService decides how each delivery is settled (real store, scripted consumer). */
class StreamIngestServiceTest {

    /** Real (non-mock) consumer driven by a queue; close() ends the stream. */
    static class ScriptedConsumer implements BatchConsumer {
        private static final Delivery SENTINEL = new RecordingDelivery(List.of(), 0);
        final BlockingQueue<Delivery> queue = new LinkedBlockingQueue<>();
        volatile boolean closed = false;

        @Override
        public void consume(Consumer<Delivery> sink) throws Exception {
            while (true) {
                Delivery d = queue.take();
                if (d == SENTINEL) return;
                sink.accept(d);
            }
        }

        @Override
        public void close() {
            closed = true;
            queue.offer(SENTINEL);
        }

        @Override
        public ConnectionState connectionState() {
            return closed ? ConnectionState.DOWN : ConnectionState.CONNECTED;
        }
    }

    /** Real Delivery that records how it was settled. */
    static final class RecordingDelivery implements Delivery {
        final List<String> outcomes = new CopyOnWriteArrayList<>();
        private final List<LsmRow> rows;
        private final int payloadBytes;

        RecordingDelivery(List<LsmRow> rows, int payloadBytes) {
            this.rows = rows;
            this.payloadBytes = payloadBytes;
        }

        @Override public List<LsmRow> rows() { return rows; }
        @Override public int payloadBytes() { return payloadBytes; }
        @Override public boolean redelivered() { return false; }
        @Override public void accept() { outcomes.add("accept"); }
        @Override public void fail() { outcomes.add("fail"); }
        @Override public void reject() { outcomes.add("reject"); }
    }

    /** A real store whose write raises: an unexpected, possibly transient storage error. */
    static final class FailingStore extends LsmStore {
        @Override
        public void ingest(List<LsmRow> batch) {
            throw new IllegalStateException("simulated storage failure");
        }
    }

    private static IngestSettings settings() {
        IngestSettings s = new IngestSettings();
        s.setMaxDisconnectSeconds(0); // watchdog + failure shutdown off
        return s;
    }

    private static List<String> settle(RecordingDelivery d, LsmStore store, IngestSettings settings) {
        ScriptedConsumer consumer = new ScriptedConsumer();
        StreamIngestService svc = new StreamIngestService(consumer, store, settings, () -> {});
        svc.start();
        try {
            consumer.queue.offer(d);
            await().atMost(2, TimeUnit.SECONDS).until(() -> !d.outcomes.isEmpty());
            return d.outcomes;
        } finally {
            svc.close();
        }
    }

    @Test
    void successfulWriteAcceptsAndLandsInStore() {
        LsmStore store = new LsmStore();
        RecordingDelivery d = new RecordingDelivery(List.of(new LsmRow(1, "a", "x", 0, "insert")), 10);
        assertThat(settle(d, store, settings())).containsExactly("accept");
        assertThat(store.query(10).total()).isEqualTo(1);
    }

    @Test
    void oversizedPayloadRejects() {
        IngestSettings s = settings();
        s.setMaxBatchBytes(1);
        LsmStore store = new LsmStore();
        RecordingDelivery d = new RecordingDelivery(List.of(new LsmRow(1, "a", "x", 0, "insert")), 10);
        assertThat(settle(d, store, s)).containsExactly("reject");
        assertThat(store.query(10).total()).isZero();
    }

    @Test
    void storeErrorFailsForRedelivery() {
        RecordingDelivery d = new RecordingDelivery(List.of(new LsmRow(1, "a", "x", 0, "insert")), 10);
        assertThat(settle(d, new FailingStore(), settings())).containsExactly("fail");
    }

    /** A Delivery whose fail() redelivers it at once, as a broker does; records when. */
    static final class RedeliveredDelivery implements Delivery {
        final List<Long> failedAtNanos = new CopyOnWriteArrayList<>();
        private final ScriptedConsumer consumer;

        RedeliveredDelivery(ScriptedConsumer consumer) {
            this.consumer = consumer;
        }

        @Override public List<LsmRow> rows() { return List.of(new LsmRow(1, "a", "x", 0, "insert")); }
        @Override public int payloadBytes() { return 10; }
        @Override public boolean redelivered() { return !failedAtNanos.isEmpty(); }
        @Override public void accept() { throw new AssertionError("accept"); }
        @Override public void reject() { throw new AssertionError("reject"); }
        @Override public void fail() {
            failedAtNanos.add(System.nanoTime());
            consumer.queue.offer(this);
        }
    }

    private static long gapMillis(List<Long> nanos, int i) {
        return TimeUnit.NANOSECONDS.toMillis(nanos.get(i) - nanos.get(i - 1));
    }

    private static long timeCloseMillis(StreamIngestService svc) {
        long t0 = System.nanoTime();
        svc.close();
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
    }

    @Test
    void persistentStoreErrorBacksOffBetweenRedeliveries() {
        ScriptedConsumer consumer = new ScriptedConsumer();
        StreamIngestService svc = new StreamIngestService(consumer, new FailingStore(), settings(), () -> {});
        RedeliveredDelivery d = new RedeliveredDelivery(consumer);
        svc.start();
        try {
            consumer.queue.offer(d);
            // fail, wait 1-1.25 s, fail, wait 2-2.5 s, fail: no hot loop.
            await().atMost(10, TimeUnit.SECONDS).until(() -> d.failedAtNanos.size() >= 3);
            assertThat(gapMillis(d.failedAtNanos, 1)).as("first backoff").isBetween(1000L, 1900L);
            assertThat(gapMillis(d.failedAtNanos, 2)).as("doubled backoff").isBetween(2000L, 3800L);
            // Now inside a 4-5 s backoff: close() must cut it short.
            assertThat(timeCloseMillis(svc)).as("close during settlement backoff (ms)").isLessThan(3000L);
            assertThat(d.failedAtNanos).as("no settlement after close").hasSize(3);
        } finally {
            svc.close(); // idempotent
        }
    }

    @Test
    void closeDuringSettlementBackoffIsPrompt() {
        ScriptedConsumer consumer = new ScriptedConsumer();
        StreamIngestService svc = new StreamIngestService(consumer, new FailingStore(), settings(), () -> {});
        RedeliveredDelivery d = new RedeliveredDelivery(consumer);
        svc.start();
        try {
            consumer.queue.offer(d);
            await().atMost(2, TimeUnit.SECONDS).until(() -> d.failedAtNanos.size() == 1);
            // The pending wait is >= 1 s; an uninterruptible one would hold close() that long.
            assertThat(timeCloseMillis(svc)).as("close (ms)").isLessThan(800L);
            assertThat(d.failedAtNanos).as("the redelivery is left for the broker").hasSize(1);
        } finally {
            svc.close(); // idempotent
        }
    }

    @Test
    void closeDuringTransportBackoffIsPrompt() {
        AtomicInteger calls = new AtomicInteger();
        ScriptedConsumer consumer = new ScriptedConsumer() {
            @Override
            public void consume(Consumer<Delivery> sink) {
                calls.incrementAndGet();
                throw new IllegalStateException("transport down");
            }
        };
        StreamIngestService svc = new StreamIngestService(consumer, new LsmStore(), settings(), () -> {});
        svc.start();
        try {
            await().atMost(2, TimeUnit.SECONDS).until(() -> calls.get() == 1);
            // The pending transport backoff is >= 1 s.
            assertThat(timeCloseMillis(svc)).as("close (ms)").isLessThan(800L);
            assertThat(calls.get()).as("no reconnect after close").isEqualTo(1);
        } finally {
            svc.close(); // idempotent
        }
    }

    /** Fails 2 calls, delivers once, fails 3 more, then blocks until closed. */
    static final class FlakyConsumer extends ScriptedConsumer {
        final AtomicInteger calls = new AtomicInteger();

        @Override
        public void consume(Consumer<Delivery> sink) throws Exception {
            int n = calls.incrementAndGet();
            if (n == 3) {
                sink.accept(new RecordingDelivery(List.of(), 0));
            }
            if (n <= 5) {
                throw new IllegalStateException("transport down (call " + n + ")");
            }
            super.consume(sink);
        }
    }

    @Test
    void aDeliveryResetsTheConsecutiveFailureCount() {
        IngestSettings s = settings();
        s.setMaxDisconnectSeconds(3600); // enables the 5-consecutive-failures shutdown
        FlakyConsumer consumer = new FlakyConsumer();
        AtomicBoolean shutdown = new AtomicBoolean();
        StreamIngestService svc = new StreamIngestService(consumer, new LsmStore(), s, () -> shutdown.set(true));
        svc.start();
        try {
            // 5 failures in total, but never 5 in a row: backoff ~1+2, reset, ~1+2+4 s.
            await().atMost(20, TimeUnit.SECONDS).until(() -> consumer.calls.get() == 6 || shutdown.get());
            assertThat(shutdown.get()).as("shutdown requested").isFalse();
        } finally {
            svc.close();
        }
    }

    /**
     * Binds and then fails, as a Solace session does after a healed terminal
     * interruption on a quiet queue (no deliveries), {@code failures} times in a
     * row; then binds and blocks until closed.
     */
    static final class BindThenFailConsumer extends ScriptedConsumer {
        final AtomicInteger calls = new AtomicInteger();
        private final int failures;

        BindThenFailConsumer(int failures) {
            this.failures = failures;
        }

        @Override
        public void consume(Consumer<Delivery> sink, Runnable onBound) throws Exception {
            int n = calls.incrementAndGet();
            onBound.run();
            if (n <= failures) {
                throw new IllegalStateException("service interrupted after bind (call " + n + ")");
            }
            super.consume(sink, onBound);
        }
    }

    @Test
    void aSuccessfulBindResetsTheConsecutiveFailureCount() {
        IngestSettings s = settings();
        s.setMaxDisconnectSeconds(3600); // enables the 5-consecutive-failures shutdown
        BindThenFailConsumer consumer = new BindThenFailConsumer(6); // > MAX_FAILURES (5)
        AtomicBoolean shutdown = new AtomicBoolean();
        StreamIngestService svc = new StreamIngestService(consumer, new LsmStore(), s, () -> shutdown.set(true));
        svc.start();
        try {
            // Each bind resets the backoff to the 1 s base: 6 waits of 1-1.25 s, not 1+2+4+8+16.
            await().atMost(15, TimeUnit.SECONDS).until(() -> consumer.calls.get() == 7 || shutdown.get());
            assertThat(shutdown.get()).as("shutdown requested").isFalse();
            assertThat(svc.currentHealth().threadAlive()).as("ingest thread alive").isTrue();
        } finally {
            svc.close();
        }
    }
}
