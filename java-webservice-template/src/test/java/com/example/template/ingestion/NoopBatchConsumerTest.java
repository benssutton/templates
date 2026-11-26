package com.example.template.ingestion;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Readiness relies on this contract: CONNECTED until closed, DOWN after. */
class NoopBatchConsumerTest {

    @Test
    void isConnectedUntilClosedThenDown() {
        NoopBatchConsumer consumer = new NoopBatchConsumer();
        assertThat(consumer.connectionState()).isEqualTo(ConnectionState.CONNECTED);

        consumer.close();

        assertThat(consumer.connectionState()).isEqualTo(ConnectionState.DOWN);
    }

    @Test
    void signalsBoundThenIdlesUntilClosed() throws Exception {
        NoopBatchConsumer consumer = new NoopBatchConsumer();
        CountDownLatch bound = new CountDownLatch(1);
        Thread t =
                Thread.ofPlatform()
                        .start(() -> {
                            try {
                                consumer.consume(d -> {}, bound::countDown);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });

        assertThat(bound.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(t.isAlive()).isTrue();

        consumer.close();
        t.join(5000);
        assertThat(t.isAlive()).isFalse();
    }
}
