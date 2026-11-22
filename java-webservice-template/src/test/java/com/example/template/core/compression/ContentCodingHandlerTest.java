package com.example.template.core.compression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdOutputStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.compression.DecompressionException;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/** The decoder in isolation: what reaches the next handler for a given request. */
class ContentCodingHandlerTest {

    private static final long MIB = 1 << 20;

    /** Collects what the handler forwards, releasing content so a bomb never accumulates. */
    private static final class Sink extends ChannelInboundHandlerAdapter {
        final ByteArrayOutputStream body = new ByteArrayOutputStream();
        long bytes;
        final List<Object> messages = new ArrayList<>();
        boolean keepBody = true;
        HttpRequest request;

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof HttpRequest r) {
                request = r;
            }
            if (msg instanceof HttpContent c) {
                bytes += c.content().readableBytes();
                if (keepBody) {
                    byte[] b = new byte[c.content().readableBytes()];
                    c.content().getBytes(c.content().readerIndex(), b);
                    body.writeBytes(b);
                }
            }
            messages.add(msg instanceof LastHttpContent ? "last" : msg.getClass().getSimpleName());
            if (msg instanceof LastHttpContent last && last.decoderResult().isFailure()) {
                messages.set(messages.size() - 1, last.decoderResult().cause());
            }
            ReferenceCountUtil.release(msg);
        }

        Throwable onlyFailure() {
            List<Object> failures = messages.stream().filter(m -> m instanceof Throwable).toList();
            assertThat(failures).hasSize(1);
            assertThat(messages.getLast()).isSameAs(failures.getFirst());
            return (Throwable) failures.getFirst();
        }
    }

    /** One connection: the handler under test and a fresh sink behind it. */
    private record Conn(EmbeddedChannel ch, Sink sink) {
        static Conn open(long maxRequestSize) {
            Sink sink = new Sink();
            return new Conn(
                    new EmbeddedChannel(new ContentCodingHandler(false, maxRequestSize), sink),
                    sink);
        }

        /** Sends {@code body} as chunks of {@code size} bytes, the last one a LastHttpContent. */
        Conn send(HttpRequest request, byte[] body, int size) {
            ch.writeInbound(request);
            for (int i = 0; i < body.length; i += size) {
                ByteBuf part = Unpooled.wrappedBuffer(body, i, Math.min(size, body.length - i));
                ch.writeInbound(
                        i + size >= body.length
                                ? new DefaultLastHttpContent(part)
                                : new DefaultHttpContent(part));
            }
            return this;
        }
    }

    private static HttpRequest post(String... encodings) {
        HttpRequest r = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/x");
        for (String e : encodings) {
            r.headers().add(HttpHeaderNames.CONTENT_ENCODING, e);
        }
        return r;
    }

    private static byte[] text(int n) {
        byte[] data = new byte[n];
        Random random = new Random(n);
        for (int i = 0; i < n; i++) {
            data[i] = (byte) ('a' + random.nextInt(8));
        }
        return data;
    }

    @Test
    void decodedBytesStopAtTheLimit() throws Exception {
        Conn c = Conn.open(MIB);
        c.sink.keepBody = false;
        byte[] bomb = CompressionFixtures.zstdZeros(50 * MIB);
        c.send(post("zstd"), bomb, bomb.length);
        // the chunk that crosses the limit is dropped, not forwarded
        assertThat(c.sink.bytes).isLessThanOrEqualTo(MIB);
        assertThat(c.sink.onlyFailure()).isInstanceOf(TooLongFrameException.class);
    }

    @Test
    void decodingStopsOnceTheLimitIsPassed() throws Exception {
        byte[][] bombs = {
            CompressionFixtures.zstdZeros(50 * MIB), CompressionFixtures.gzipZeros(50 * MIB)
        };
        String[] codings = {"zstd", "gzip"};
        // zstd emits fixed 64 KiB chunks; gzip's chunk size follows Netty's inflate buffer sizing
        long[] overshoot = {64 * 1024, 256 * 1024};
        for (int i = 0; i < bombs.length; i++) {
            ContentCodingHandler.Decoding d =
                    ContentCodingHandler.open(codings[i], MIB, new EmbeddedChannel());
            byte[] bomb = bombs[i];
            assertThatThrownBy(() -> d.channel().writeInbound(Unpooled.wrappedBuffer(bomb)))
                    .as(codings[i])
                    .isInstanceOf(DecoderException.class);
            // about one output chunk past the limit, not the rest of the 50 MiB
            assertThat(d.budget().decodedBytes())
                    .as(codings[i])
                    .isLessThanOrEqualTo(MIB + overshoot[i]);
            // closing may report the refused allocation again
            catchThrowable(() -> d.channel().finishAndReleaseAll());
        }
    }

    @Test
    void aBodyAtTheLimitPasses() throws Exception {
        byte[] body = new byte[4096];
        Conn c = Conn.open(body.length).send(post("zstd"), Zstd.compress(body, 3), 7);
        assertThat(c.sink.body.toByteArray()).isEqualTo(body);
        assertThat(c.sink.messages.getLast()).isEqualTo("last");
    }

    @Test
    void zstdAtTheHighestLevelStillDecodes() throws Exception {
        byte[] body = text(3 << 20);
        Conn c = Conn.open(4 * MIB).send(post("zstd"), Zstd.compress(body, 19), 8192);
        assertThat(c.sink.body.toByteArray()).isEqualTo(body);
        assertThat(c.sink.messages.getLast()).isEqualTo("last");
    }

    @Test
    void aZstdWindowAbove8MibIsRefused() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ZstdOutputStream z = new ZstdOutputStream(out, 3)) {
            z.setWindowLog(25);
            z.write(text(1000));
        }
        Conn c = Conn.open(MIB).send(post("zstd"), out.toByteArray(), 8192);
        // a malformed-body failure (400), not the size limit (413)
        assertThat(c.sink.onlyFailure())
                .isInstanceOf(DecompressionException.class)
                .isNotInstanceOf(TooLongFrameException.class);
    }

    @Test
    void afterAFailureTheRestOfTheBodyIsDropped() {
        byte[] garbage = "not gzip ".repeat(1000).getBytes(StandardCharsets.UTF_8);
        Conn c = Conn.open(MIB).send(post("gzip"), garbage, 512);
        assertThat(c.sink.onlyFailure()).isInstanceOf(DecoderException.class);

        // the next request on the connection starts clean
        int before = c.sink.messages.size();
        byte[] body = "hello".getBytes(StandardCharsets.UTF_8);
        c.send(post("zstd"), Zstd.compress(body, 3), 1024);
        assertThat(c.sink.messages.subList(before, c.sink.messages.size()))
                .containsExactly("DefaultHttpRequest", "DefaultHttpContent", "last");
    }

    @Test
    void truncatedBodiesFail() throws Exception {
        byte[] body = "hello world".repeat(200).getBytes(StandardCharsets.UTF_8);
        for (String coding : List.of("zstd", "gzip")) {
            byte[] whole =
                    coding.equals("zstd") ? Zstd.compress(body, 3) : CompressionFixtures.gzip(body);
            for (int cut : new int[] {1, 4, 5, whole.length / 2, whole.length - 1}) {
                Conn c =
                        Conn.open(MIB)
                                .send(post(coding), Arrays.copyOf(whole, whole.length - cut), 16);
                assertThat(c.sink.onlyFailure())
                        .as(coding + " minus " + cut)
                        .isInstanceOf(DecoderException.class);
            }
        }
    }

    @Test
    void bytesAfterTheGzipTrailerFail() throws Exception {
        byte[] member = CompressionFixtures.gzip("hello".getBytes(StandardCharsets.UTF_8));
        byte[] two = Arrays.copyOf(member, member.length * 2);
        System.arraycopy(member, 0, two, member.length, member.length);
        Conn c = Conn.open(MIB).send(post("gzip"), two, 1024);
        assertThat(c.sink.onlyFailure()).isInstanceOf(DecoderException.class);
    }

    @Test
    void wholeBodiesInManyChunksDecode() throws Exception {
        byte[] body = "hello world".repeat(200).getBytes(StandardCharsets.UTF_8);
        byte[] frame = Zstd.compress(body, 3);
        byte[] twoFrames = Arrays.copyOf(frame, frame.length * 2);
        System.arraycopy(frame, 0, twoFrames, frame.length, frame.length);
        Conn z = Conn.open(MIB).send(post("zstd"), twoFrames, 3);
        assertThat(z.sink.body.toString(StandardCharsets.UTF_8))
                .isEqualTo(new String(body, StandardCharsets.UTF_8).repeat(2));
        assertThat(z.sink.messages.getLast()).isEqualTo("last");

        Conn g = Conn.open(MIB).send(post("gzip"), CompressionFixtures.gzip(body), 3);
        assertThat(g.sink.body.toByteArray()).isEqualTo(body);
        assertThat(g.sink.messages.getLast()).isEqualTo("last");
    }

    @Test
    void contentEncodingLinesAreJoined() {
        Conn c = Conn.open(MIB);
        c.ch.writeInbound(post("gzip", "zstd"));
        assertThat(c.sink.request.headers().getAll(HttpHeaderNames.CONTENT_ENCODING))
                .containsExactly("gzip, zstd");
        // no decoder for a stack: the body passes through for the filter's 415
        c.ch.writeInbound(new DefaultLastHttpContent(Unpooled.wrappedBuffer(new byte[] {1, 2})));
        assertThat(c.sink.body.toByteArray()).containsExactly(1, 2);
    }
}
