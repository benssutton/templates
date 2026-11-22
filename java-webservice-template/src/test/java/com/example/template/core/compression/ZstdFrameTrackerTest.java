package com.example.template.core.compression;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.Test;

class ZstdFrameTrackerTest {

    private static final byte[] MAGIC = {0x28, (byte) 0xB5, 0x2F, (byte) 0xFD};

    /** Feeds the chunks in order and reports whether the input ends between frames. */
    private static boolean atBoundary(byte[]... chunks) {
        ZstdFrameTracker tracker = new ZstdFrameTracker();
        EmbeddedChannel ch = new EmbeddedChannel(tracker);
        for (byte[] chunk : chunks) {
            ch.writeInbound(Unpooled.wrappedBuffer(chunk));
        }
        ch.finishAndReleaseAll();
        return tracker.atFrameBoundary();
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] p : parts) {
            out.writeBytes(p);
        }
        return out.toByteArray();
    }

    /** Little-endian block header: last flag, type (0 raw, 1 RLE, 3 reserved), size. */
    private static byte[] blockHeader(boolean last, int type, int size) {
        int v = (last ? 1 : 0) | type << 1 | size << 3;
        return new byte[] {(byte) v, (byte) (v >> 8), (byte) (v >> 16)};
    }

    /** Single-segment frame header with a one-byte content size and no checksum. */
    private static byte[] header(int contentSize) {
        return concat(MAGIC, new byte[] {0x20, (byte) contentSize});
    }

    private static byte[] skippable(int size) {
        byte[] head = {0x53, 0x2A, 0x4D, 0x18, (byte) size, 0, 0, 0};
        return concat(head, new byte[size]);
    }

    private static byte[] withChecksum(byte[] data) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ZstdOutputStream z = new ZstdOutputStream(out, 3)) {
            z.setChecksum(true);
            z.write(data);
        }
        return out.toByteArray();
    }

    private static final byte[] TEXT = "hello world ".repeat(100).getBytes(StandardCharsets.UTF_8);

    @Test
    void emptyInputIsABoundary() {
        assertThat(atBoundary()).isTrue();
    }

    @Test
    void checksummedFrameEndsOnlyAfterItsChecksum() throws IOException {
        byte[] frame = withChecksum(TEXT);
        assertThat(frame[4] & 0x04).as("checksum flag").isNotZero();
        assertThat(atBoundary(frame)).isTrue();
        for (int cut = 1; cut <= 4; cut++) {
            assertThat(atBoundary(Arrays.copyOf(frame, frame.length - cut)))
                    .as("minus " + cut)
                    .isFalse();
        }
    }

    @Test
    void rleBlockCarriesOneByte() {
        byte[] frame = concat(header(200), blockHeader(true, 1, 200), new byte[] {'a'});
        assertThat(Zstd.decompress(frame, 200))
                .isEqualTo("a".repeat(200).getBytes(StandardCharsets.UTF_8));
        assertThat(atBoundary(frame)).isTrue();
        assertThat(atBoundary(Arrays.copyOf(frame, frame.length - 1))).isFalse();
    }

    @Test
    void handBuiltMultiBlockFrame() {
        byte[] frame =
                concat(
                        header(7),
                        blockHeader(false, 0, 3),
                        "abc".getBytes(StandardCharsets.UTF_8),
                        blockHeader(true, 1, 4),
                        new byte[] {'d'});
        assertThat(Zstd.decompress(frame, 7)).isEqualTo("abcdddd".getBytes(StandardCharsets.UTF_8));
        assertThat(atBoundary(frame)).isTrue();
        assertThat(atBoundary(Arrays.copyOf(frame, frame.length - 2))).isFalse();
    }

    @Test
    void libraryMultiBlockFrame() {
        // random letters do not compress to a single block: 300 KB spans three 128 KiB blocks
        byte[] data = new byte[300_000];
        Random random = new Random(1);
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) ('a' + random.nextInt(26));
        }
        byte[] frame = Zstd.compress(data, 3);
        assertThat(atBoundary(frame)).isTrue();
        assertThat(atBoundary(Arrays.copyOf(frame, frame.length / 2))).isFalse();
        assertThat(atBoundary(Arrays.copyOf(frame, frame.length - 1))).isFalse();
    }

    @Test
    void skippableFramesAreSkipped() {
        byte[] frame = Zstd.compress(TEXT, 3);
        assertThat(atBoundary(skippable(0))).isTrue();
        assertThat(atBoundary(concat(skippable(5), frame, skippable(0)))).isTrue();
        assertThat(atBoundary(Arrays.copyOf(skippable(5), 10))).isFalse();
    }

    @Test
    void reservedBlockTypeIsCorrupt() {
        byte[] bad = concat(header(0), blockHeader(true, 3, 0));
        assertThat(atBoundary(bad)).isFalse();
        // nothing after a corrupt block can bring the stream back to a boundary
        assertThat(atBoundary(bad, Zstd.compress(TEXT, 3))).isFalse();
    }

    @Test
    void unknownMagicIsCorrupt() {
        assertThat(atBoundary(new byte[] {1, 2, 3, 4})).isFalse();
    }

    @Test
    void headerFedOneByteAtATime() throws IOException {
        byte[] frame = withChecksum(TEXT);
        byte[][] chunks = new byte[frame.length][];
        for (int i = 0; i < frame.length; i++) {
            chunks[i] = new byte[] {frame[i]};
        }
        assertThat(atBoundary(chunks)).isTrue();
        assertThat(atBoundary(Arrays.copyOf(chunks, frame.length - 1))).isFalse();
    }

    @Test
    void cutExactlyAtAFrameBoundaryIsAccepted() throws IOException {
        byte[] first = withChecksum(TEXT);
        byte[] second = Zstd.compress(TEXT, 3);
        assertThat(atBoundary(first, second)).isTrue();
        assertThat(atBoundary(first)).isTrue();
        assertThat(atBoundary(first, Arrays.copyOf(second, 2))).isFalse();
    }
}
