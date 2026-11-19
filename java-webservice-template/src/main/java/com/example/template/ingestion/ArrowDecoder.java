package com.example.template.ingestion;

import com.example.template.config.IngestSettings;
import com.example.template.persistence.streamstore.LsmRow;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

/**
 * Decodes Arrow IPC stream bytes into LSM rows. Expects columns id (int64) and op (utf8, one of
 * {@link #OPS}), both non-null, plus nullable name and value (utf8). Anything else is a {@link
 * MalformedBatchException}; so is a record batch whose Arrow buffers exceed {@code
 * max-batch-bytes}.
 *
 * <p>Every stream is fully decoded and validated before the caller writes anything, so a bad batch
 * never leaves an earlier batch of the same stream half-ingested.
 *
 * <p>One root allocator, bounded by {@code template.ingest.decoder-memory-bytes}, is shared by all
 * decodes (HTTP requests and the Solace thread), so concurrent decoding cannot exhaust direct
 * memory; past the bound a decode fails with {@link OutOfMemoryException}, which HTTP answers with
 * 503.
 */
@Singleton
public class ArrowDecoder implements AutoCloseable {

    /** Allowed values of the op column: an upsert or a delete tombstone. */
    public static final Set<String> OPS = Set.of("upsert", "delete");

    private final long maxBatchBytes;
    private final BufferAllocator allocator;

    @Inject
    public ArrowDecoder(IngestSettings settings) {
        this(settings.maxBatchBytes(), settings.decoderMemoryBytes());
    }

    public ArrowDecoder(long maxBatchBytes, long memoryLimitBytes) {
        this.maxBatchBytes = maxBatchBytes;
        this.allocator = new RootAllocator(memoryLimitBytes);
    }

    public List<LsmRow> decodeAll(byte[] ipcBytes) {
        List<LsmRow> rows = new ArrayList<>();
        try (BufferAllocator child =
                        allocator.newChildAllocator("decode", 0, allocator.getLimit());
                ArrowStreamReader reader =
                        new ArrowStreamReader(new ByteArrayInputStream(ipcBytes), child)) {
            VectorSchemaRoot root = readSchema(reader);
            while (loadNextBatch(reader)) {
                checkSize(root);
                rows.addAll(decodeRoot(root));
            }
        } catch (IOException e) {
            throw new MalformedBatchException.Unreadable(
                    "not an Arrow IPC stream: " + e.getMessage(), e);
        }
        return rows;
    }

    private void checkSize(VectorSchemaRoot root) {
        long bytes = root.getFieldVectors().stream().mapToLong(FieldVector::getBufferSize).sum();
        if (bytes > maxBatchBytes) {
            throw new MalformedBatchException.TooLarge(
                    "batch of " + bytes + " bytes exceeds max-batch-bytes (" + maxBatchBytes + ")");
        }
    }

    static List<LsmRow> decodeRoot(VectorSchemaRoot root) {
        BigIntVector id = column(root, "id", BigIntVector.class, "int64");
        VarCharVector name = column(root, "name", VarCharVector.class, "utf8");
        VarCharVector value = column(root, "value", VarCharVector.class, "utf8");
        VarCharVector op = column(root, "op", VarCharVector.class, "utf8");
        int n = root.getRowCount();
        List<LsmRow> rows = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            if (id.isNull(i) || op.isNull(i)) {
                throw new MalformedBatchException.Invalid(
                        "row " + i + ": id and op must not be null");
            }
            String o = text(op, i);
            if (!OPS.contains(o)) {
                throw new MalformedBatchException.Invalid(
                        "row " + i + ": op must be one of " + OPS + ", got '" + o + "'");
            }
            rows.add(new LsmRow(id.get(i), text(name, i), text(value, i), 0, o));
        }
        return rows;
    }

    private static <V extends FieldVector> V column(
            VectorSchemaRoot root, String name, Class<V> type, String arrowType) {
        FieldVector vector = root.getVector(name);
        if (vector == null) {
            throw new MalformedBatchException.Invalid("batch missing required column: " + name);
        }
        if (!type.isInstance(vector)) {
            throw new MalformedBatchException.Invalid(
                    "column "
                            + name
                            + " must be "
                            + arrowType
                            + ", got "
                            + vector.getField().getType());
        }
        return type.cast(vector);
    }

    private static String text(VarCharVector vector, int i) {
        return vector.isNull(i) ? null : new String(vector.get(i), StandardCharsets.UTF_8);
    }

    /** The reader fails lazily on garbage bytes, with IOException or a runtime error. */
    private static VectorSchemaRoot readSchema(ArrowStreamReader reader) throws IOException {
        try {
            return reader.getVectorSchemaRoot();
        } catch (OutOfMemoryException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new MalformedBatchException.Unreadable(
                    "not an Arrow IPC stream: " + e.getMessage(), e);
        }
    }

    private static boolean loadNextBatch(ArrowStreamReader reader) throws IOException {
        try {
            return reader.loadNextBatch();
        } catch (OutOfMemoryException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new MalformedBatchException.Unreadable(
                    "corrupt Arrow IPC batch: " + e.getMessage(), e);
        }
    }

    /** Direct memory currently held by in-flight decodes. */
    public long allocatedBytes() {
        return allocator.getAllocatedMemory();
    }

    @Override
    @PreDestroy
    public void close() {
        allocator.close();
    }
}
