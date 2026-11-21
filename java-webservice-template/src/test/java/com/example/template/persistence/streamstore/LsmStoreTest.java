package com.example.template.persistence.streamstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class LsmStoreTest {

    private static LsmRow upsert(long id, String value) {
        return new LsmRow(id, "n" + id, value, 0, "upsert");
    }

    private static LsmRow delete(long id) {
        return new LsmRow(id, "n" + id, "", 0, "delete");
    }

    private static List<Long> ids(LsmStore.QueryResult r) {
        return r.rows().stream().map(LsmRow::id).toList();
    }

    @ParameterizedTest(name = "flushRows={0}, compactionRuns={1}")
    @CsvSource({"100, 4", "1, 2", "2, 2", "1, 100"})
    void newestVersionWinsAndTombstonesHide(int flushRows, int compactionRuns) {
        LsmStore store = new LsmStore(flushRows, compactionRuns, 1_000);
        store.ingest(List.of(upsert(3, "v1"), upsert(1, "v1"), upsert(2, "v1")));
        store.ingest(List.of(upsert(1, "v2"), delete(2)));

        LsmStore.QueryResult r = store.query(10);

        assertThat(ids(r)).containsExactly(1L, 3L);
        assertThat(r.rows().getFirst().value()).isEqualTo("v2");
        assertThat(r.total()).isEqualTo(2);
    }

    @Test
    void reinsertAfterDeleteIsLive() {
        LsmStore store = new LsmStore(1, 2, 1_000);
        store.ingest(List.of(upsert(1, "v1")));
        store.ingest(List.of(delete(1)));
        store.ingest(List.of(upsert(1, "v3")));
        assertThat(store.query(10).rows()).extracting(LsmRow::value).containsExactly("v3");
    }

    @Test
    void limitTruncatesButTotalCountsAllLiveRows() {
        LsmStore store = new LsmStore(100, 4, 1_000);
        store.ingest(List.of(upsert(5, "a"), upsert(4, "a"), upsert(6, "a")));
        LsmStore.QueryResult r = store.query(2);
        assertThat(ids(r)).containsExactly(4L, 5L);
        assertThat(r.total()).isEqualTo(3);
    }

    @Test
    void overTheCapTheOldestLiveRowsAreEvicted() {
        // Cap 10, low-water mark 10 - max(1, 10 / 10) = 9.
        LsmStore store = new LsmStore(1, 2, 10);
        for (long id = 1; id <= 10; id++) {
            store.ingest(List.of(upsert(id, "a")));
        }
        assertThat(store.evictedRows()).isZero();

        store.ingest(List.of(upsert(11, "a")));

        LsmStore.QueryResult r = store.query(100);
        assertThat(ids(r)).containsExactly(3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L);
        assertThat(r.total()).isLessThanOrEqualTo(9);
        assertThat(store.evictedRows()).isEqualTo(2);
    }

    /** Oldest-first and a re-written key is young again: its new seqno protects it. */
    @Test
    void evictionGoesBySeqnoNotKey() {
        LsmStore store = new LsmStore(100, 4, 10);
        for (long id = 1; id <= 10; id++) {
            store.ingest(List.of(upsert(id, "a")));
        }
        store.ingest(List.of(upsert(1, "b"))); // still 10 live rows: no eviction
        assertThat(store.evictedRows()).isZero();

        store.ingest(List.of(upsert(11, "a")));

        assertThat(ids(store.query(100))).containsExactly(1L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L);
        assertThat(store.evictedRows()).isEqualTo(2);
    }

    /**
     * Overwrites push stored rows (shadowed versions included) past the cap, but live rows stay
     * under it: compaction alone suffices and no live row is evicted.
     */
    @Test
    void overwritesThatCrossTheCapEvictNothing() {
        // flushRows 10 and compactionRuns 1000: every 10 rows become a run, and runs only merge
        // at the cap, so shadowed versions accumulate across runs.
        LsmStore store = new LsmStore(10, 1_000, 1_000);
        for (long id = 1; id <= 950; id += 10) {
            List<LsmRow> batch = new ArrayList<>();
            for (long k = id; k < id + 10; k++) {
                batch.add(upsert(k, "a"));
            }
            store.ingest(batch);
        }
        // 200 overwrites: stored rows reach 1010 after the 6th batch (crossing the cap at 1000),
        // and cross again later. Only 950 live rows throughout.
        for (long id = 1; id <= 200; id += 10) {
            List<LsmRow> batch = new ArrayList<>();
            for (long k = id; k < id + 10; k++) {
                batch.add(upsert(k, "b"));
            }
            store.ingest(batch);
        }

        LsmStore.QueryResult r = store.query(1_000);
        assertThat(store.evictedRows()).isZero();
        assertThat(r.total()).isEqualTo(950);
        assertThat(r.rows())
                .filteredOn(row -> row.id() <= 200)
                .extracting(LsmRow::value)
                .hasSize(200)
                .containsOnly("b");
        assertThat(r.rows())
                .filteredOn(row -> row.id() > 200)
                .extracting(LsmRow::value)
                .containsOnly("a");
    }

    /** Cap 100, low-water 90: one-row ingests evict 11 rows every 11th ingest, not every time. */
    @Test
    void evictionIsAmortisedNotPerIngest() {
        LsmStore store = new LsmStore(1_000, 4, 100);
        List<Long> steps = new ArrayList<>();
        long before = 0;
        for (long id = 1; id <= 150; id++) {
            store.ingest(List.of(upsert(id, "a")));
            long now = store.evictedRows();
            if (now != before) {
                steps.add(now - before);
                before = now;
            }
            assertThat(store.query(1).total()).isLessThanOrEqualTo(100);
        }
        // Evictions at ids 101, 112, 123, 134 and 145.
        assertThat(steps).containsExactly(11L, 11L, 11L, 11L, 11L);
        assertThat(store.evictedRows()).isEqualTo(55);
        LsmStore.QueryResult r = store.query(1);
        assertThat(r.total()).isEqualTo(95);
        assertThat(ids(r)).containsExactly(56L);
    }

    @Test
    void invalidSizesAreRefused() {
        assertThatThrownBy(() -> new LsmStore(0, 2, 10))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new LsmStore(1, 1, 10))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new LsmStore(1, 2, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
