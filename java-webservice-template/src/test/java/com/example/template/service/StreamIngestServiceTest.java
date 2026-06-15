package com.example.template.service;

import com.example.template.config.IngestSettings;
import com.example.template.ingestion.BatchConsumer;
import com.example.template.ingestion.ConnectionState;
import com.example.template.persistence.streamstore.LsmRow;
import com.example.template.persistence.streamstore.LsmStore;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class StreamIngestServiceTest {

    /** Real (non-mock) consumer driven by a queue; an empty-list sentinel ends the stream. */
    static class FakeConsumer implements BatchConsumer {
        final BlockingQueue<List<LsmRow>> queue = new LinkedBlockingQueue<>();
        volatile boolean closed = false;
        private static final List<LsmRow> SENTINEL = List.of();

        @Override
        public void consume(Consumer<List<LsmRow>> sink) throws Exception {
            while (true) {
                List<LsmRow> batch = queue.take();
                if (batch == SENTINEL) return;
                sink.accept(batch);
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

    private IngestSettings settings() {
        IngestSettings s = new IngestSettings();
        s.setMaxDisconnectSeconds(0); // disable watchdog/failure shutdown in the test
        return s;
    }

    @Test
    void ingestedBatchesLandInTheStore() throws Exception {
        FakeConsumer consumer = new FakeConsumer();
        LsmStore store = new LsmStore();
        StreamIngestService svc = new StreamIngestService(consumer, store, settings(), () -> {});
        svc.start();

        consumer.queue.offer(List.of(new LsmRow(1, "a", "x", 0, "insert")));
        await().atMost(2, TimeUnit.SECONDS).until(() -> store.query(10).total() == 1);

        assertThat(store.query(10).rows().get(0).id()).isEqualTo(1L);
        assertThat(svc.currentHealth().connectionState()).isEqualTo("connected");
        svc.close();
    }
}
