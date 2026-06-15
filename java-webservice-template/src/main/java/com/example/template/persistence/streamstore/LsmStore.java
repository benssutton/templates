package com.example.template.persistence.streamstore;

import java.util.ArrayList;
import java.util.List;

/**
 * Simplified append-only stream store.
 *
 * <p>SIMPLIFIED FROM THE PYTHON TEMPLATE: the polars window-function merge /
 * compaction is removed. Rows are appended with a monotonic seqno and returned
 * as-is (duplicates and delete tombstones included); the client receiving the
 * data performs compaction.
 *
 * <p>SINGLE-WRITER CONTRACT: exactly one thread (the ingest consumer thread)
 * calls {@link #ingest}. Readers ({@link #query}) are lock-free: each append
 * publishes a new immutable snapshot via the {@code volatile} reference, so a
 * reader always sees a consistent list.
 */
public final class LsmStore {

    public record QueryResult(List<LsmRow> rows, long total) {}

    private volatile List<LsmRow> snapshot = List.of();
    private long seqno = 0; // writer-private

    public void ingest(List<LsmRow> batch) {
        List<LsmRow> next = new ArrayList<>(snapshot);
        for (LsmRow row : batch) {
            next.add(new LsmRow(row.id(), row.name(), row.value(), seqno++, row.op()));
        }
        snapshot = List.copyOf(next); // atomic publish of immutable snapshot
    }

    public QueryResult query(int limit) {
        List<LsmRow> snap = snapshot; // atomic read
        long total = snap.size();
        List<LsmRow> rows = limit < snap.size() ? List.copyOf(snap.subList(0, limit)) : snap;
        return new QueryResult(rows, total);
    }
}
