package com.example.template.core.compression;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.luben.zstd.Zstd;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.TooLongFrameException;
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
import org.junit.jupiter.api.Test;

/** The decoder in isolation: what reaches the next handler for a given request. */
class ContentCodingHandlerTest {

    /** Collects what the handler forwards, releasing content so a bomb never accumulates. */
    @ChannelHandler.Sharable
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
    }

    private final Sink sink = new Sink();

    private EmbeddedChannel channel(long maxRequestSize) {
        return new EmbeddedChannel(new ContentCodingHandler(false, maxRequestSize), sink);
    }

    private static HttpRequest post(String... encodings) {
        HttpRequest r = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/x");
        for (String e : encodings) {
            r.headers().add(HttpHeaderNames.CONTENT_ENCODING, e);
        }
        return r;
    }

    /** Sends {@code body} as chunks of {@code size} bytes, the last one a LastHttpContent. */
    private static void send(EmbeddedChannel ch, HttpRequest request, byte[] body, int size) {
        ch.writeInbound(request);
        for (int i = 0; i < body.length; i += size) {
            ByteBuf part = Unpooled.wrappedBuffer(body, i, Math.min(size, body.length - i));
            ch.writeInbound(
                    i + size >= body.length
                            ? new DefaultLastHttpContent(part)
                            : new DefaultHttpContent(part));
        }
    }

    private Throwable onlyFailure() {
        List<Object> failures = sink.messages.stream().filter(m -> m instanceof Throwable).toList();
        assertThat(failures).hasSize(1);
        assertThat(sink.messages.getLast()).isSameAs(failures.getFirst());
        return (Throwable) failures.getFirst();
    }

    @Test
    void decodedBytesStopAtTheLimit() throws Exception {
        sink.keepBody = false;
        long limit = 1 << 20;
        byte[] bomb = CompressionFixtures.zstdZeros(200L << 20);
        send(channel(limit), post("zstd"), bomb, bomb.length);
        // the chunk that crosses the limit is dropped, not forwarded
        assertThat(sink.bytes).isLessThanOrEqualTo(limit);
        assertThat(onlyFailure()).isInstanceOf(TooLongFrameException.class);
    }

    @Test
    void aBodyAtTheLimitPasses() throws Exception {
        byte[] body = new byte[4096];
        send(channel(body.length), post("zstd"), Zstd.compress(body, 3), 7);
        assertThat(sink.body.toByteArray()).isEqualTo(body);
        assertThat(sink.messages.getLast()).isEqualTo("last");
    }

    @Test
    void afterAFailureTheRestOfTheBodyIsDropped() {
        byte[] garbage = "not gzip ".repeat(1000).getBytes(StandardCharsets.UTF_8);
        EmbeddedChannel ch = channel(1 << 20);
        send(ch, post("gzip"), garbage, 512);
        assertThat(onlyFailure()).isInstanceOf(DecoderException.class);

        // the next request on the connection starts clean
        sink.messages.clear();
        byte[] body = "hello".getBytes(StandardCharsets.UTF_8);
        send(ch, post("zstd"), Zstd.compress(body, 3), 1024);
        assertThat(sink.messages)
                .containsExactly("DefaultHttpRequest", "DefaultHttpContent", "last");
    }

    @Test
    void truncatedBodiesFail() throws Exception {
        byte[] body = "hello world".repeat(200).getBytes(StandardCharsets.UTF_8);
        for (String coding : List.of("zstd", "gzip")) {
            byte[] whole =
                    coding.equals("zstd") ? Zstd.compress(body, 3) : CompressionFixtures.gzip(body);
            for (int cut : new int[] {1, 4, 5, whole.length / 2, whole.length - 1}) {
                sink.messages.clear();
                send(channel(1 << 20), post(coding), Arrays.copyOf(whole, whole.length - cut), 16);
                assertThat(onlyFailure())
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
        send(channel(1 << 20), post("gzip"), two, 1024);
        assertThat(onlyFailure()).isInstanceOf(DecoderException.class);
    }

    @Test
    void wholeBodiesInManyChunksDecode() throws Exception {
        byte[] body = "hello world".repeat(200).getBytes(StandardCharsets.UTF_8);
        byte[] twoFrames = new byte[0];
        for (byte[] frame : List.of(Zstd.compress(body, 3), Zstd.compress(body, 3))) {
            byte[] joined = Arrays.copyOf(twoFrames, twoFrames.length + frame.length);
            System.arraycopy(frame, 0, joined, twoFrames.length, frame.length);
            twoFrames = joined;
        }
        send(channel(1 << 20), post("zstd"), twoFrames, 3);
        assertThat(sink.body.toString(StandardCharsets.UTF_8))
                .isEqualTo(new String(body, StandardCharsets.UTF_8).repeat(2));
        assertThat(sink.messages.getLast()).isEqualTo("last");

        sink.body.reset();
        send(channel(1 << 20), post("gzip"), CompressionFixtures.gzip(body), 3);
        assertThat(sink.body.toByteArray()).isEqualTo(body);
        assertThat(sink.messages.getLast()).isEqualTo("last");
    }

    @Test
    void contentEncodingLinesAreJoined() {
        EmbeddedChannel ch = channel(1 << 20);
        ch.writeInbound(post("gzip", "zstd"));
        assertThat(sink.request.headers().getAll(HttpHeaderNames.CONTENT_ENCODING))
                .containsExactly("gzip, zstd");
        // no decoder for a stack: the body passes through for the filter's 415
        ch.writeInbound(new DefaultLastHttpContent(Unpooled.wrappedBuffer(new byte[] {1, 2})));
        assertThat(sink.body.toByteArray()).containsExactly(1, 2);
    }
}
