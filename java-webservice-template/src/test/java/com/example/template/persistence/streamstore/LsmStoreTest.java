package com.example.template.persistence.streamstore;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class LsmStoreTest {

    @Test
    void appendsAssignSeqnoAndQueryReturnsInOrderWithOpAndSeqno() {
        LsmStore store = new LsmStore();
        store.ingest(List.of(new LsmRow(1, "a", "x", 0, "insert"),
                             new LsmRow(2, "b", "y", 0, "insert")));
        store.ingest(List.of(new LsmRow(1, "a", "x2", 0, "insert")));

        LsmStore.QueryResult r = store.query(10);
        assertThat(r.total()).isEqualTo(3);           // append-only: no dedup
        assertThat(r.rows()).extracting(LsmRow::seqno).containsExactly(0L, 1L, 2L);
        assertThat(r.rows()).extracting(LsmRow::id).containsExactly(1L, 2L, 1L);
        assertThat(r.rows().get(2).value()).isEqualTo("x2");
    }

    @Test
    void queryRespectsLimit() {
        LsmStore store = new LsmStore();
        for (int i = 0; i < 5; i++) store.ingest(List.of(new LsmRow(i, "n", "v", 0, "insert")));
        LsmStore.QueryResult r = store.query(2);
        assertThat(r.rows()).hasSize(2);
        assertThat(r.total()).isEqualTo(5);
    }

    @Test
    void preservesDeleteTombstonesAsPlainRows() {
        LsmStore store = new LsmStore();
        store.ingest(List.of(new LsmRow(1, "a", "x", 0, "insert"),
                             new LsmRow(1, "a", "x", 0, "delete")));
        LsmStore.QueryResult r = store.query(10);
        assertThat(r.rows()).extracting(LsmRow::op).containsExactly("insert", "delete");
    }
}
