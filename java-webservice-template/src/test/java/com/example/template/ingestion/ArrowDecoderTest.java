package com.example.template.ingestion;

import static com.example.template.support.ArrowBatches.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.template.persistence.streamstore.LsmRow;
import com.example.template.support.ArrowBatches;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.memory.OutOfMemoryException;
import org.junit.jupiter.api.Test;

class ArrowDecoderTest {

    private static final long MIB = 1024 * 1024;

    @Test
    void decodesIpcBytesToRows() {
        byte[] ipc =
                ArrowBatches.encode(
                        List.of(
                                new LsmRow(1, "a", "x", 0, "upsert"),
                                new LsmRow(2, null, "y", 0, "delete")));
        try (ArrowDecoder decoder = new ArrowDecoder(MIB, 16 * MIB)) {
            List<LsmRow> rows = decoder.decodeAll(ipc);
            assertThat(rows).extracting(LsmRow::id).containsExactly(1L, 2L);
            assertThat(rows).extracting(LsmRow::op).containsExactly("upsert", "delete");
            assertThat(rows.get(1).name()).isNull();
            assertThat(rows.get(1).value()).isEqualTo("y");
            assertThat(decoder.allocatedBytes()).as("buffers released").isZero();
        }
    }

    @Test
    void emptyOrGarbageBytesAreUnreadable() {
        try (ArrowDecoder decoder = new ArrowDecoder(MIB, 16 * MIB)) {
            for (byte[] bad :
                    List.of(new byte[0], "not arrow ipc".getBytes(StandardCharsets.UTF_8))) {
                assertThatThrownBy(() -> decoder.decodeAll(bad))
                        .isInstanceOf(MalformedBatchException.Unreadable.class);
            }
            assertThat(decoder.allocatedBytes()).isZero();
        }
    }

    @Test
    void unknownOpIsInvalid() {
        try (ArrowDecoder decoder = new ArrowDecoder(MIB, 16 * MIB)) {
            assertThatThrownBy(
                            () -> decoder.decodeAll(ArrowBatches.encode(List.of(row(1, "insert")))))
                    .isInstanceOf(MalformedBatchException.Invalid.class)
                    .hasMessageContaining("op");
        }
    }

    /** Any batch of the stream over the limit fails the whole stream, before rows are returned. */
    @Test
    void batchOverMaxBatchBytesIsTooLarge() {
        byte[] ipc =
                ArrowBatches.encodeBatches(
                        List.of(
                                List.of(row(1, "upsert")),
                                java.util.stream.LongStream.range(0, 2000)
                                        .mapToObj(i -> row(i, "upsert"))
                                        .toList()));
        try (ArrowDecoder decoder = new ArrowDecoder(4096, 16 * MIB)) {
            assertThatThrownBy(() -> decoder.decodeAll(ipc))
                    .isInstanceOf(MalformedBatchException.TooLarge.class);
            assertThat(decoder.allocatedBytes()).isZero();
        }
    }

    /** The shared allocator is bounded: decoding beyond it fails instead of exhausting memory. */
    @Test
    void sharedAllocatorIsBounded() {
        byte[] ipc =
                ArrowBatches.encode(
                        java.util.stream.LongStream.range(0, 2000)
                                .mapToObj(i -> row(i, "upsert"))
                                .toList());
        try (ArrowDecoder decoder = new ArrowDecoder(MIB, 1024)) {
            assertThatThrownBy(() -> decoder.decodeAll(ipc))
                    .isInstanceOf(OutOfMemoryException.class);
        }
    }
}
