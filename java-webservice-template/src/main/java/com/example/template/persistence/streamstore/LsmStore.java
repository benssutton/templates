package com.example.template.persistence.streamstore;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simplified append-only stream store.
 *
 * <p>Unlike the Python template there is no merge or compaction: rows are appended with a monotonic
 * seqno and returned as-is (duplicates and delete tombstones included); the client receiving the
 * data performs compaction.
 *
 * <p>Bounded: without compaction the store would grow forever, so it retains at most {@code
 * maxRows} rows ({@code template.ingest.lsm-max-rows}) and evicts the oldest beyond that. Evictions
 * are counted ({@link #evictedRows()}, exported as a metric and on the ingest health) and logged at
 * WARN at most once a minute, because silently losing rows is the one thing a cache consumer cannot
 * detect on its own.
 *
 * <p>Concurrency: HTTP ingest and the transport ingest thread both write, so every method holds the
 * monitor. An append costs O(batch) and a query O(limit), so the monitor is never held for a pass
 * over the whole store.
 */
public class LsmStore {

    private static final Logger LOG = LoggerFactory.getLogger(LsmStore.class);
    private static final long EVICTION_WARN_INTERVAL_NANOS = Duration.ofMinutes(1).toNanos();

    /**
     * {@code rows} are the newest {@code limit} rows in seqno order; {@code total} counts all
     * retained rows.
     */
    public record QueryResult(List<LsmRow> rows, long total) {}

    /** Retention cap used when none is configured (matches application.yml). */
    public static final int DEFAULT_MAX_ROWS = 1_000_000;

    private final int maxRows;
    private final ArrayDeque<LsmRow> rows = new ArrayDeque<>();
    private long seqno;
    private long evicted;
    private long evictedSinceWarn;
    private long lastWarnNanos = System.nanoTime() - EVICTION_WARN_INTERVAL_NANOS;

    public LsmStore() {
        this(DEFAULT_MAX_ROWS);
    }

    public LsmStore(int maxRows) {
        if (maxRows < 1) {
            throw new IllegalArgumentException("maxRows must be >= 1, was " + maxRows);
        }
        this.maxRows = maxRows;
    }

    public synchronized void ingest(List<LsmRow> batch) {
        long dropped = 0;
        for (LsmRow row : batch) {
            rows.addLast(new LsmRow(row.id(), row.name(), row.value(), seqno++, row.op()));
            if (rows.size() > maxRows) {
                rows.removeFirst();
                dropped++;
            }
        }
        if (dropped > 0) {
            recordEvictions(dropped);
        }
    }

    private void recordEvictions(long dropped) {
        evicted += dropped;
        evictedSinceWarn += dropped;
        long now = System.nanoTime();
        if (now - lastWarnNanos >= EVICTION_WARN_INTERVAL_NANOS) {
            LOG.warn(
                    "LSM store at its {}-row cap: evicted {} oldest rows since the last warning ({} in total)",
                    maxRows,
                    evictedSinceWarn,
                    evicted);
            evictedSinceWarn = 0;
            lastWarnNanos = now;
        }
    }

    public synchronized QueryResult query(int limit) {
        int n = Math.min(Math.max(limit, 0), rows.size());
        List<LsmRow> newest = new ArrayList<>(n);
        Iterator<LsmRow> it = rows.descendingIterator();
        for (int i = 0; i < n; i++) {
            newest.add(it.next());
        }
        Collections.reverse(newest);
        return new QueryResult(Collections.unmodifiableList(newest), rows.size());
    }

    /** Rows dropped by the retention cap since start. */
    public synchronized long evictedRows() {
        return evicted;
    }
}
