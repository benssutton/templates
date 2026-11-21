package com.example.template.persistence.streamstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        LsmStore store = new LsmStore(1, 2, 2);
        store.ingest(List.of(upsert(1, "a")));
        store.ingest(List.of(upsert(2, "a")));
        store.ingest(List.of(upsert(3, "a")));
        assertThat(ids(store.query(10))).containsExactly(2L, 3L);
        assertThat(store.evictedRows()).isEqualTo(1);
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
