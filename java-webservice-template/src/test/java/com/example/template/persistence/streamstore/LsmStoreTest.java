package com.example.template.persistence.streamstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

class LsmStoreTest {

    @Test
    void appendsAssignSeqnoAndQueryReturnsInOrderWithOpAndSeqno() {
        LsmStore store = new LsmStore();
        store.ingest(
                List.of(
                        new LsmRow(1, "a", "x", 0, "upsert"),
                        new LsmRow(2, "b", "y", 0, "upsert")));
        store.ingest(List.of(new LsmRow(1, "a", "x2", 0, "upsert")));

        LsmStore.QueryResult r = store.query(10);
        assertThat(r.total()).isEqualTo(3); // append-only: no dedup
        assertThat(r.rows()).extracting(LsmRow::seqno).containsExactly(0L, 1L, 2L);
        assertThat(r.rows()).extracting(LsmRow::id).containsExactly(1L, 2L, 1L);
        assertThat(r.rows().get(2).value()).isEqualTo("x2");
    }

    /** A tailing client wants the latest rows: the newest {@code limit}, oldest first. */
    @Test
    void queryReturnsTheNewestRowsUpToLimit() {
        LsmStore store = new LsmStore();
        for (int i = 0; i < 5; i++) store.ingest(List.of(new LsmRow(i, "n", "v", 0, "upsert")));
        LsmStore.QueryResult r = store.query(2);
        assertThat(r.rows()).extracting(LsmRow::seqno).containsExactly(3L, 4L);
        assertThat(r.total()).isEqualTo(5);
    }

    @Test
    void evictionsAreCounted() {
        LsmStore store = new LsmStore(3);
        store.ingest(
                List.of(
                        new LsmRow(1, "n", "v", 0, "upsert"),
                        new LsmRow(2, "n", "v", 0, "upsert")));
        assertThat(store.evictedRows()).isZero();
        store.ingest(
                List.of(
                        new LsmRow(3, "n", "v", 0, "upsert"),
                        new LsmRow(4, "n", "v", 0, "upsert")));
        assertThat(store.evictedRows()).isEqualTo(1);
        // A batch larger than the cap keeps only its newest rows; the rest count as evicted.
        store.ingest(
                List.of(
                        new LsmRow(5, "n", "v", 0, "upsert"),
                        new LsmRow(6, "n", "v", 0, "upsert"),
                        new LsmRow(7, "n", "v", 0, "upsert"),
                        new LsmRow(8, "n", "v", 0, "upsert")));
        assertThat(store.evictedRows()).isEqualTo(5);
        assertThat(store.query(10).rows()).extracting(LsmRow::id).containsExactly(6L, 7L, 8L);
    }

    @Test
    void querySnapshotIsUnaffectedByLaterWrites() {
        LsmStore store = new LsmStore(2);
        store.ingest(List.of(new LsmRow(1, "n", "v", 0, "upsert")));
        List<LsmRow> rows = store.query(10).rows();
        store.ingest(
                List.of(
                        new LsmRow(2, "n", "v", 0, "upsert"),
                        new LsmRow(3, "n", "v", 0, "upsert")));
        assertThat(rows).extracting(LsmRow::id).containsExactly(1L);
        assertThatThrownBy(() -> rows.add(rows.get(0)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void concurrentIngestLosesNoRowsAndAssignsUniqueSeqnos() throws Exception {
        // HTTP ingest (blocking executor) and the transport thread both write.
        LsmStore store = new LsmStore();
        int writers = 16, batchesPerWriter = 200;
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int w = 0; w < writers; w++) {
                pool.submit(() -> {
                    for (int b = 0; b < batchesPerWriter; b++) {
                        store.ingest(List.of(new LsmRow(b, "n", "v", 0, "upsert")));
                    }
                });
            }
        }
        LsmStore.QueryResult r = store.query(Integer.MAX_VALUE);
        assertThat(r.total()).isEqualTo((long) writers * batchesPerWriter);
        assertThat(r.rows()).extracting(LsmRow::seqno).doesNotHaveDuplicates();
    }

    @Test
    void evictsOldestRowsBeyondMaxRows() {
        LsmStore store = new LsmStore(3);
        for (int i = 0; i < 5; i++) store.ingest(List.of(new LsmRow(i, "n", "v", 0, "upsert")));
        LsmStore.QueryResult r = store.query(10);
        assertThat(r.total()).isEqualTo(3);
        assertThat(r.rows()).extracting(LsmRow::seqno).containsExactly(2L, 3L, 4L);
    }

    @Test
    void rejectsNonPositiveMaxRows() {
        assertThatThrownBy(() -> new LsmStore(0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void preservesDeleteTombstonesAsPlainRows() {
        LsmStore store = new LsmStore();
        store.ingest(
                List.of(
                        new LsmRow(1, "a", "x", 0, "upsert"),
                        new LsmRow(1, "a", "x", 0, "delete")));
        LsmStore.QueryResult r = store.query(10);
        assertThat(r.rows()).extracting(LsmRow::op).containsExactly("upsert", "delete");
    }
}
