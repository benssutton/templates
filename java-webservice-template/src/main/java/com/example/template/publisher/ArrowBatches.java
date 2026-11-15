package com.example.template.publisher;

import com.example.template.persistence.streamstore.LsmRow;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** Encodes rows as one Arrow IPC stream batch: the wire format every transport carries. */
public final class ArrowBatches {

    private static final Schema SCHEMA = new Schema(List.of(
        new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null),
        new Field("name", FieldType.notNullable(new ArrowType.Utf8()), null),
        new Field("value", FieldType.notNullable(new ArrowType.Utf8()), null),
        new Field("op", FieldType.notNullable(new ArrowType.Utf8()), null)));

    private ArrowBatches() {}

    /** seqno on the input rows is ignored (the store assigns it). */
    public static byte[] encode(List<LsmRow> rows) {
        try (RootAllocator alloc = new RootAllocator();
             VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, alloc)) {
            BigIntVector id = (BigIntVector) root.getVector("id");
            VarCharVector name = (VarCharVector) root.getVector("name");
            VarCharVector value = (VarCharVector) root.getVector("value");
            VarCharVector op = (VarCharVector) root.getVector("op");
            for (int i = 0; i < rows.size(); i++) {
                LsmRow r = rows.get(i);
                id.setSafe(i, r.id());
                name.setSafe(i, r.name().getBytes(StandardCharsets.UTF_8));
                value.setSafe(i, r.value().getBytes(StandardCharsets.UTF_8));
                op.setSafe(i, r.op().getBytes(StandardCharsets.UTF_8));
            }
            root.setRowCount(rows.size());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ArrowStreamWriter w = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
                w.start();
                w.writeBatch();
                w.end();
            }
            return out.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("arrow encode failed", e);
        }
    }
}
