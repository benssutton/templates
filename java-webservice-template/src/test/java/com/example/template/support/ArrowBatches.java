package com.example.template.support;

import com.example.template.persistence.streamstore.LsmRow;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Encodes rows as an Arrow IPC stream: the wire format every transport carries. Used by the tests
 * and by the demo publisher. A null name or value is written as a null cell.
 */
public final class ArrowBatches {

    private static final Schema SCHEMA =
            new Schema(
                    List.of(
                            new Field(
                                    "id", FieldType.notNullable(new ArrowType.Int(64, true)), null),
                            new Field("name", FieldType.nullable(new ArrowType.Utf8()), null),
                            new Field("value", FieldType.nullable(new ArrowType.Utf8()), null),
                            new Field("op", FieldType.notNullable(new ArrowType.Utf8()), null)));

    private ArrowBatches() {}

    /** One record batch; seqno on the input rows is ignored (the store assigns it). */
    public static byte[] encode(List<LsmRow> rows) {
        return encodeBatches(List.of(rows));
    }

    public static LsmRow row(long id, String op) {
        return new LsmRow(id, "n" + id, "v" + id, 0, op);
    }

    /** One IPC stream holding each list as its own record batch. */
    public static byte[] encodeBatches(List<List<LsmRow>> batches) {
        try (RootAllocator alloc = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, alloc)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ArrowStreamWriter w =
                    new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
                w.start();
                for (List<LsmRow> rows : batches) {
                    fill(root, rows);
                    w.writeBatch();
                }
                w.end();
            }
            return out.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("arrow encode failed", e);
        }
    }

    private static void fill(VectorSchemaRoot root, List<LsmRow> rows) {
        root.allocateNew();
        BigIntVector id = (BigIntVector) root.getVector("id");
        VarCharVector name = (VarCharVector) root.getVector("name");
        VarCharVector value = (VarCharVector) root.getVector("value");
        VarCharVector op = (VarCharVector) root.getVector("op");
        for (int i = 0; i < rows.size(); i++) {
            LsmRow r = rows.get(i);
            id.setSafe(i, r.id());
            text(name, i, r.name());
            text(value, i, r.value());
            op.setSafe(i, r.op().getBytes(StandardCharsets.UTF_8));
        }
        root.setRowCount(rows.size());
    }

    private static void text(VarCharVector v, int i, String s) {
        if (s == null) {
            v.setNull(i);
        } else {
            v.setSafe(i, s.getBytes(StandardCharsets.UTF_8));
        }
    }
}
