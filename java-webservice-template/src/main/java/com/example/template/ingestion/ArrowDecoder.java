package com.example.template.ingestion;

import com.example.template.persistence.streamstore.LsmRow;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Decodes Arrow IPC stream bytes (and live VectorSchemaRoots) into LSM rows.
 * Expects columns id (int64), name, value, op (utf8).
 */
public final class ArrowDecoder implements AutoCloseable {

    private final RootAllocator allocator = new RootAllocator();

    public List<LsmRow> decodeAll(byte[] ipcBytes) throws Exception {
        List<LsmRow> rows = new ArrayList<>();
        try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipcBytes), allocator)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                rows.addAll(decodeRoot(root));
            }
        }
        return rows;
    }

    public static List<LsmRow> decodeRoot(VectorSchemaRoot root) {
        BigIntVector id = (BigIntVector) root.getVector("id");
        VarCharVector name = (VarCharVector) root.getVector("name");
        VarCharVector value = (VarCharVector) root.getVector("value");
        VarCharVector op = (VarCharVector) root.getVector("op");
        int n = root.getRowCount();
        List<LsmRow> rows = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            rows.add(new LsmRow(
                id.get(i),
                new String(name.get(i), StandardCharsets.UTF_8),
                new String(value.get(i), StandardCharsets.UTF_8),
                0,
                new String(op.get(i), StandardCharsets.UTF_8)));
        }
        return rows;
    }

    @Override
    public void close() {
        allocator.close();
    }
}
