package com.example.template.ingestion.solace;

import com.example.template.config.IngestSettings;
import com.example.template.ingestion.ConnectionState;
import com.example.template.ingestion.Delivery;
import com.example.template.persistence.streamstore.LsmRow;
import com.example.template.publisher.ArrowBatches;
import com.example.template.support.SolaceSupport;
import com.example.template.support.SolaceSupport.Names;
import com.solace.messaging.PubSubPlusClientException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.example.template.support.SolaceSupport.SEMP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/** SolaceBatchConsumer against a real broker; broker-side facts asserted via SEMP. */
class SolaceBatchConsumerTest {

    private final Names names = Names.unique();
    private final BlockingQueue<Delivery> received = new LinkedBlockingQueue<>();
    private final AtomicReference<Exception> consumeError = new AtomicReference<>();
    private SolaceBatchConsumer consumer;
    private Thread consumeThread;
    /** Set by tests where consume() is meant to fail; otherwise a silent failure fails the test. */
    private boolean expectConsumeError;

    private SolaceBatchConsumer start(IngestSettings settings) {
        consumer = new SolaceBatchConsumer(settings);
        consumeOnThread(received::add);
        return consumer;
    }

    private void consumeOnThread(Consumer<Delivery> sink) {
        consumeError.set(null);
        consumeThread = new Thread(() -> {
            try {
                consumer.consume(sink);
            } catch (Exception e) {
                consumeError.set(e);
            }
        }, "solace-consume");
        consumeThread.setDaemon(true);
        consumeThread.start();
    }

    private void awaitQueue() {
        await().atMost(30, TimeUnit.SECONDS).until(() -> SEMP.queueExists(names.queue()));
    }

    private void awaitBound() {
        awaitQueue();
        await().atMost(30, TimeUnit.SECONDS).until(() -> SEMP.queueBindCount(names.queue()) == 1);
    }

    private static byte[] oneRow(long id) {
        return ArrowBatches.encode(List.of(new LsmRow(id, "n", "v", 0, "upsert")));
    }

    private Delivery next() throws InterruptedException {
        Delivery d = received.poll(15, TimeUnit.SECONDS);
        assertThat(d).as("delivery within 15s").isNotNull();
        return d;
    }

    @AfterEach
    void stop() throws InterruptedException {
        if (consumer == null) return;
        consumer.close();
        if (consumeThread != null) {
            consumeThread.join(10_000);
            assertThat(consumeThread.isAlive()).as("consume() returns after close()").isFalse();
        }
        if (!expectConsumeError) {
            assertThat(consumeError.get()).as("consume() failed").isNull();
        }
    }

    @Test
    void startProvisionsDurableQueueWithTopicSubscription() {
        start(SolaceSupport.settings(names));
        awaitQueue();
        assertThat(SEMP.queueSubscriptions(names.queue())).containsExactly(names.topic());
    }

    @Test
    void acceptRemovesMessageFromQueue() throws Exception {
        start(SolaceSupport.settings(names));
        awaitQueue();
        SolaceSupport.publish(names.topic(), oneRow(5));
        Delivery d = next();
        assertThat(d.rows()).extracting(LsmRow::id).containsExactly(5L);
        assertThat(d.redelivered()).isFalse();
        assertThat(SEMP.queueMessageCount(names.queue())).isEqualTo(1); // unacked: still spooled
        d.accept();
        await().atMost(10, TimeUnit.SECONDS).until(() -> SEMP.queueMessageCount(names.queue()) == 0);
    }

    @Test
    void failRedeliversMessage() throws Exception {
        start(SolaceSupport.settings(names));
        awaitQueue();
        SolaceSupport.publish(names.topic(), oneRow(6));
        next().fail();
        Delivery again = next();
        assertThat(again.redelivered()).isTrue();
        assertThat(again.rows()).extracting(LsmRow::id).containsExactly(6L);
        again.accept();
    }

    @Test
    void rejectRemovesMessageWithoutRedelivery() throws Exception {
        start(SolaceSupport.settings(names));
        awaitQueue();
        SolaceSupport.publish(names.topic(), oneRow(7));
        next().reject();
        await().atMost(10, TimeUnit.SECONDS).until(() -> SEMP.queueMessageCount(names.queue()) == 0);
        assertThat(received.poll(2, TimeUnit.SECONDS)).isNull();
    }

    @Test
    void malformedPayloadIsRejectedByTheConsumer() throws Exception {
        start(SolaceSupport.settings(names));
        awaitQueue();
        SolaceSupport.publish(names.topic(), "definitely not arrow ipc".getBytes(StandardCharsets.UTF_8));
        await().atMost(10, TimeUnit.SECONDS).until(() -> SEMP.queueMessageCount(names.queue()) == 0);
        assertThat(received.poll(2, TimeUnit.SECONDS)).isNull(); // never handed to the ingest loop
    }

    @Test
    void reconnectsAndResumesAfterNetworkPartition() throws Exception {
        start(SolaceSupport.settings(names));
        awaitQueue();
        SolaceSupport.pause();
        try {
            // Keepalives go unanswered; forever-retry means RECONNECTING, never a terminal interruption.
            await().atMost(45, TimeUnit.SECONDS).until(() -> consumer.connectionState() == ConnectionState.RECONNECTING);
        } finally {
            SolaceSupport.unpause();
        }
        await().atMost(45, TimeUnit.SECONDS).until(() -> consumer.connectionState() == ConnectionState.CONNECTED);
        SolaceSupport.publish(names.topic(), oneRow(8)); // the persistent flow re-binds automatically
        Delivery d = received.poll(30, TimeUnit.SECONDS);
        assertThat(d).isNotNull();
        assertThat(d.rows()).extracting(LsmRow::id).containsExactly(8L);
        d.accept();
    }

    @Test
    void bindFailsWhenClientProfileDeniesQueueCreation() {
        expectConsumeError = true;
        String user = "u-" + names.queue();
        SEMP.createEndpointDeniedUser("deny-" + names.queue(), user, "pw");
        IngestSettings s = SolaceSupport.settings(names);
        s.getSolace().setUsername(user);
        s.getSolace().setPassword("pw");
        start(s);
        await().atMost(20, TimeUnit.SECONDS).until(() -> consumeError.get() != null);
        assertThat(consumeError.get()).isInstanceOf(PubSubPlusClientException.class);
        assertThat(SEMP.queueExists(names.queue())).isFalse();
        // The failed call disconnected its service: a retry starts from a fresh connection.
        await().atMost(10, TimeUnit.SECONDS).until(() -> SEMP.clientCount(consumer.clientName()) == 0);
    }

    @Test
    void closeBeforeConsumeReturnsWithoutConnecting() {
        consumer = new SolaceBatchConsumer(SolaceSupport.settings(names));
        consumer.close();
        assertTimeoutPreemptively(Duration.ofSeconds(1), () -> consumer.consume(received::add));
        assertThat(SEMP.clientCount(consumer.clientName())).isZero();
        assertThat(SEMP.queueExists(names.queue())).isFalse();
    }

    @Test
    void closeUnbindsQueueAndDisconnects() throws Exception {
        start(SolaceSupport.settings(names));
        awaitBound();
        assertThat(SEMP.clientCount(consumer.clientName())).isEqualTo(1);
        consumer.close();
        consumeThread.join(10_000);
        assertThat(consumeThread.isAlive()).isFalse();
        assertThat(consumeError.get()).isNull(); // a close is a clean return, not a failure
        await().atMost(10, TimeUnit.SECONDS).until(() -> SEMP.queueBindCount(names.queue()) == 0);
        await().atMost(10, TimeUnit.SECONDS).until(() -> SEMP.clientCount(consumer.clientName()) == 0);
    }

    @Test
    void closeStopsHandingBufferedDeliveriesToTheSink() throws Exception {
        consumer = new SolaceBatchConsumer(SolaceSupport.settings(names));
        AtomicInteger sinkCalls = new AtomicInteger();
        CountDownLatch release = new CountDownLatch(1);
        consumeOnThread(d -> {
            sinkCalls.incrementAndGet();
            try {
                release.await(); // a slow store write, holding the ingest thread
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        awaitBound();
        for (long id = 1; id <= 3; id++) SolaceSupport.publish(names.topic(), oneRow(id));
        // All 3 delivered to the client: 1 in the sink, 2 buffered behind it.
        await().atMost(15, TimeUnit.SECONDS).until(() -> SEMP.queueUnackedCount(names.queue()) == 3);
        consumer.close();
        release.countDown();
        consumeThread.join(10_000);
        assertThat(consumeThread.isAlive()).isFalse();
        assertThat(sinkCalls.get()).as("sink calls after close").isEqualTo(1);
        // Nothing was settled: all 3 stay spooled for redelivery on the next bind.
        assertThat(SEMP.queueMessageCount(names.queue())).isEqualTo(3);
    }

    @Test
    void consumeAfterSinkFailureReconnectsAndRebindsExclusiveQueue() throws Exception {
        consumer = new SolaceBatchConsumer(SolaceSupport.settings(names));
        consumeOnThread(d -> { throw new IllegalStateException("store down"); });
        awaitBound();
        SolaceSupport.publish(names.topic(), oneRow(9));
        consumeThread.join(15_000);
        assertThat(consumeError.get()).hasMessage("store down");
        // The failed call released its exclusive bind and its connection...
        await().atMost(10, TimeUnit.SECONDS).until(() -> SEMP.queueBindCount(names.queue()) == 0);
        await().atMost(10, TimeUnit.SECONDS).until(() -> SEMP.clientCount(consumer.clientName()) == 0);
        // ...so the ingest loop's next call binds again and gets the unsettled message back.
        consumeOnThread(received::add);
        Delivery again = next();
        assertThat(again.rows()).extracting(LsmRow::id).containsExactly(9L);
        assertThat(again.redelivered()).isTrue();
        again.accept();
    }
}
