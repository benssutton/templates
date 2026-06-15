package com.example.template.ingestion;

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
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ArrowDecoderTest {

    @Test
    void decodesIpcBytesToRows() throws Exception {
        byte[] ipc;
        try (RootAllocator allocator = new RootAllocator()) {
            Schema schema = new Schema(List.of(
                new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null),
                new Field("name", FieldType.notNullable(new ArrowType.Utf8()), null),
                new Field("value", FieldType.notNullable(new ArrowType.Utf8()), null),
                new Field("op", FieldType.notNullable(new ArrowType.Utf8()), null)));
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                BigIntVector id = (BigIntVector) root.getVector("id");
                VarCharVector name = (VarCharVector) root.getVector("name");
                VarCharVector value = (VarCharVector) root.getVector("value");
                VarCharVector op = (VarCharVector) root.getVector("op");
                id.allocateNew(2);
                name.allocateNew();
                value.allocateNew();
                op.allocateNew();
                id.set(0, 1);
                name.setSafe(0, "a".getBytes(StandardCharsets.UTF_8));
                value.setSafe(0, "x".getBytes(StandardCharsets.UTF_8));
                op.setSafe(0, "insert".getBytes(StandardCharsets.UTF_8));
                id.set(1, 2);
                name.setSafe(1, "b".getBytes(StandardCharsets.UTF_8));
                value.setSafe(1, "y".getBytes(StandardCharsets.UTF_8));
                op.setSafe(1, "delete".getBytes(StandardCharsets.UTF_8));
                root.setRowCount(2);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                try (ArrowStreamWriter w = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
                    w.start();
                    w.writeBatch();
                    w.end();
                }
                ipc = out.toByteArray();
            }
        }

        List<LsmRow> rows;
        try (ArrowDecoder decoder = new ArrowDecoder()) {
            rows = decoder.decodeAll(ipc);
        }
        assertThat(rows).extracting(LsmRow::id).containsExactly(1L, 2L);
        assertThat(rows).extracting(LsmRow::op).containsExactly("insert", "delete");
        assertThat(rows.get(1).value()).isEqualTo("y");
    }
}
