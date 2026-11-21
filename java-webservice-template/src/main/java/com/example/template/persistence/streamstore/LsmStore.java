package com.example.template.persistence.streamstore;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-memory LSM store: memtable, sorted runs and full compaction, newest version per key wins.
 *
 * <p>One writer at a time ({@code ingest} is synchronized); readers never lock, they read an
 * immutable {@link Snapshot} published at the end of every write. Delete tombstones shadow older
 * versions until a full compaction, where they are dropped. Over {@code maxRows} the writer
 * compacts everything, then evicts the oldest live rows and counts them.
 */
public class LsmStore {

    private static final Logger LOG = LoggerFactory.getLogger(LsmStore.class);
    private static final long EVICTION_WARN_INTERVAL_NANOS = Duration.ofMinutes(1).toNanos();

    /**
     * {@code rows}: live rows in key order, at most {@code limit}; {@code total}: all live rows.
     */
    public record QueryResult(List<LsmRow> rows, long total) {}

    /** Frames newest first; the merged live view is built once, on the first query. */
    private static final class Snapshot {
        private final List<NavigableMap<Long, LsmRow>> frames;
        private List<LsmRow> live;

        Snapshot(List<NavigableMap<Long, LsmRow>> frames) {
            this.frames = frames;
        }

        synchronized List<LsmRow> live() {
            if (live == null) {
                live = List.copyOf(liveRows(merge(frames)));
            }
            return live;
        }
    }

    private final int flushRows;
    private final int compactionRuns;
    private final int maxRows;
    private final AtomicLong evicted = new AtomicLong();

    // Writer state, guarded by `this`.
    private TreeMap<Long, LsmRow> memtable = new TreeMap<>();
    private final List<NavigableMap<Long, LsmRow>> runs = new ArrayList<>(); // newest first
    private long seqno;
    private long evictedSinceWarn;
    private long lastWarnNanos = System.nanoTime() - EVICTION_WARN_INTERVAL_NANOS;

    private volatile Snapshot snapshot = new Snapshot(List.of());

    public LsmStore(int flushRows, int compactionRuns, int maxRows) {
        if (flushRows < 1 || compactionRuns < 2 || maxRows < 1) {
            throw new IllegalArgumentException(
                    "need flushRows >= 1, compactionRuns >= 2, maxRows >= 1; got %d, %d, %d"
                            .formatted(flushRows, compactionRuns, maxRows));
        }
        this.flushRows = flushRows;
        this.compactionRuns = compactionRuns;
        this.maxRows = maxRows;
    }

    public synchronized void ingest(List<LsmRow> batch) {
        for (LsmRow row : batch) {
            memtable.put(row.id(), row.withSeqno(seqno++));
        }
        if (memtable.size() >= flushRows) {
            runs.addFirst(Collections.unmodifiableNavigableMap(memtable));
            memtable = new TreeMap<>();
            if (runs.size() >= compactionRuns) {
                compact();
            }
        }
        if (storedRows() > maxRows) {
            compact();
            evictOldest();
        }
        publish();
    }

    public QueryResult query(int limit) {
        List<LsmRow> live = snapshot.live();
        return new QueryResult(
                live.subList(0, Math.min(Math.max(limit, 0), live.size())), live.size());
    }

    /** Live rows dropped by the {@code maxRows} cap since start. */
    public long evictedRows() {
        return evicted.get();
    }

    private void compact() {
        if (!memtable.isEmpty()) {
            runs.addFirst(Collections.unmodifiableNavigableMap(memtable));
            memtable = new TreeMap<>();
        }
        TreeMap<Long, LsmRow> merged = merge(runs);
        merged.values().removeIf(LsmRow::isDelete);
        runs.clear();
        if (!merged.isEmpty()) {
            runs.add(merged);
        }
    }

    private void evictOldest() {
        if (runs.isEmpty()) {
            return;
        }
        NavigableMap<Long, LsmRow> only = runs.getFirst();
        int excess = only.size() - maxRows;
        if (excess <= 0) {
            return;
        }
        TreeMap<Long, LsmRow> kept = new TreeMap<>(only);
        only.values().stream()
                .sorted(Comparator.comparingLong(LsmRow::seqno))
                .limit(excess)
                .forEach(r -> kept.remove(r.id()));
        runs.set(0, kept);
        recordEvictions(excess);
    }

    private int storedRows() {
        return memtable.size() + runs.stream().mapToInt(NavigableMap::size).sum();
    }

    private void publish() {
        List<NavigableMap<Long, LsmRow>> frames = new ArrayList<>(runs.size() + 1);
        frames.add(Collections.unmodifiableNavigableMap(new TreeMap<>(memtable)));
        frames.addAll(runs);
        snapshot = new Snapshot(List.copyOf(frames));
    }

    /** Newest-first frames to one map: the first frame holding a key wins. */
    private static TreeMap<Long, LsmRow> merge(List<NavigableMap<Long, LsmRow>> newestFirst) {
        TreeMap<Long, LsmRow> merged = new TreeMap<>();
        for (NavigableMap<Long, LsmRow> frame : newestFirst) {
            frame.forEach(merged::putIfAbsent);
        }
        return merged;
    }

    private static List<LsmRow> liveRows(TreeMap<Long, LsmRow> merged) {
        return merged.values().stream().filter(r -> !r.isDelete()).toList();
    }

    private void recordEvictions(long dropped) {
        evicted.addAndGet(dropped);
        evictedSinceWarn += dropped;
        long now = System.nanoTime();
        if (now - lastWarnNanos >= EVICTION_WARN_INTERVAL_NANOS) {
            LOG.warn(
                    "LSM store at its {}-row cap: evicted {} oldest rows since the last warning ({} in total)",
                    maxRows,
                    evictedSinceWarn,
                    evicted.get());
            evictedSinceWarn = 0;
            lastWarnNanos = now;
        }
    }
}
