package com.example.template.core.compression;

import io.micronaut.core.annotation.Nullable;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.compression.DecompressionException;
import io.netty.handler.codec.compression.JdkZlibDecoder;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.codec.compression.ZstdDecoder;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpContentDecoder;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.LastHttpContent;
import java.util.List;
import java.util.Locale;
import java.util.function.BooleanSupplier;

/**
 * Inbound, right after the HTTP codec. Decodes zstd and gzip request bodies as they stream in.
 * Decoded bytes are counted against max-request-size here, inside the decoder: Netty expands
 * everything one network chunk yields before Micronaut's own limit could act, so a small chunk
 * could otherwise grow to hundreds of MB. Past the limit decoding stops (DecodeBudget), the rest is
 * released, not forwarded, and the request fails with 413; memory stays within the limit plus about
 * one decoder output chunk (64 KiB for zstd).
 *
 * <p>A corrupt or truncated body fails with 400 (Micronaut's content-decoder-failure path, which
 * also closes the connection); the rest of that body is then dropped. Bodies in any other coding,
 * including a stack such as {@code gzip, zstd}, pass through with their Content-Encoding intact for
 * CompressionPolicyFilter to refuse with 415.
 *
 * <p>Also rewrites Accept-Encoding to the single winning coding (ContentCoding's rules: a {@code *}
 * entry counts, a missing header means identity), because Netty's compressor after this handler
 * takes zstd whenever it is listed, ignoring q-values. A request that strict mode refuses loses the
 * header, so the filter's 406 goes out uncompressed.
 */
final class ContentCodingHandler extends HttpContentDecoder {

    // Netty's ZstdDecoder defaults to 4 MiB output chunks; 64 KiB keeps the overshoot past the
    // limit small.
    private static final int OUTPUT_CHUNK = 64 * 1024;
    // An 8 MiB window is what zstd levels up to 19 use; Netty's default of 128 MiB would let each
    // request hold that much decoder memory. Frames needing more (levels 20-22, long mode) get 400.
    private static final int MAX_WINDOW_LOG = 23;

    private final boolean requireZstd;
    private final long maxRequestSize;

    // The current request's decoder; null while the body is not being decoded.
    @Nullable private Decoding decoding;
    private long compressedBytes;
    private boolean failed;
    private boolean dropped;

    /** One request body's decoder channel, its budget and its end-of-stream check. */
    record Decoding(EmbeddedChannel channel, DecodeBudget budget, BooleanSupplier complete) {}

    ContentCodingHandler(boolean requireZstd, long maxRequestSize) {
        this.requireZstd = requireZstd;
        this.maxRequestSize = maxRequestSize;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, HttpObject msg, List<Object> out)
            throws Exception {
        // HttpServerCodec emits a request and its content separately, never a FullHttpRequest.
        if (msg instanceof HttpRequest request) {
            failed = false;
            decoding = null;
            joinContentEncoding(request.headers());
            normaliseAcceptEncoding(request);
        } else if (failed) {
            dropped = true; // MessageToMessageDecoder releases it
            return;
        }
        try {
            Decoding current = decoding;
            if (current != null && msg instanceof HttpContent content) {
                decodeContent(ctx, content, current, out);
            } else {
                super.decode(ctx, msg, out);
            }
        } catch (DecoderException e) {
            Decoding current = decoding;
            failed = true;
            discardDecoder();
            // Past the limit the decoder is stopped by a refused allocation; report the limit.
            DecoderException cause =
                    current != null && current.budget().exceeded() ? current.budget().failure() : e;
            // Micronaut answers a content decoder failure with 400 (413 for TooLongFrameException)
            // and closes the connection.
            DefaultLastHttpContent failure = new DefaultLastHttpContent();
            failure.setDecoderResult(DecoderResult.failure(cause));
            out.add(failure);
        }
    }

    private void decodeContent(
            ChannelHandlerContext ctx, HttpContent content, Decoding current, List<Object> out)
            throws Exception {
        ByteBuf bytes = content.content();
        compressedBytes += bytes.readableBytes();
        if (!(content instanceof LastHttpContent last)) {
            super.decode(ctx, content, out);
            checkBudget(current);
            return;
        }
        // Decode the final bytes before finishing, so a cut-off stream fails the request; the
        // decoders themselves accept one silently.
        super.decode(ctx, new DefaultHttpContent(bytes), out);
        checkBudget(current);
        // An empty body is accepted: there is nothing to decode, and some clients send the header
        // on bodiless requests.
        if (compressedBytes > 0 && !current.complete().getAsBoolean()) {
            throw new DecompressionException("Truncated compressed request body");
        }
        decoding = null;
        super.decode(ctx, last.replace(Unpooled.EMPTY_BUFFER), out);
    }

    private static void checkBudget(Decoding current) {
        if (current.budget().exceeded()) {
            throw current.budget().failure();
        }
    }

    /**
     * Closes a failed decoder now: left open, it would be finished when the next request arrives
     * and could throw again, failing that request too.
     */
    private void discardDecoder() {
        Decoding broken = decoding;
        decoding = null;
        if (broken != null) {
            try {
                broken.channel().finishAndReleaseAll();
            } catch (Exception alreadyReported) {
                // the request has failed with the first error
            }
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        super.channelReadComplete(ctx);
        // A dropped chunk produced nothing downstream, so nothing else would ask for more.
        if (dropped) {
            dropped = false;
            if (!ctx.channel().config().isAutoRead()) {
                ctx.read();
            }
        }
    }

    /** One value, so a coding stacked across header lines is seen whole (and refused). */
    private static void joinContentEncoding(HttpHeaders headers) {
        List<String> codings = headers.getAll(HttpHeaderNames.CONTENT_ENCODING);
        if (codings.size() > 1) {
            headers.set(HttpHeaderNames.CONTENT_ENCODING, String.join(", ", codings));
        }
    }

    private void normaliseAcceptEncoding(HttpRequest request) {
        HttpHeaders headers = request.headers();
        ContentCoding.Weights weights =
                ContentCoding.parse(headers.getAll(HttpHeaderNames.ACCEPT_ENCODING));
        // CompressionPolicyFilter answers strict mode's 406 when this leaves no Accept-Encoding,
        // so the header must stay removed exactly when strict mode refuses the request.
        headers.remove(HttpHeaderNames.ACCEPT_ENCODING);
        if (weights == null
                || ContentCoding.strictRefuses(
                        requireZstd,
                        request.method().name(),
                        ContentCoding.path(request.uri()),
                        weights)) {
            return;
        }
        String value = weights.winningCoding();
        if (value != null) {
            headers.set(HttpHeaderNames.ACCEPT_ENCODING, value);
        }
    }

    @Override
    protected EmbeddedChannel newContentDecoder(String contentEncoding) {
        decoding = open(contentEncoding, maxRequestSize, ctx.channel());
        compressedBytes = 0;
        return decoding == null ? null : decoding.channel();
    }

    /** The decoder channel for a zstd or gzip body, or null for any other coding. */
    @Nullable
    static Decoding open(String contentEncoding, long maxBytes, Channel parent) {
        String coding = contentEncoding.strip().toLowerCase(Locale.ROOT);
        if (!coding.equals("zstd") && !coding.equals("gzip")) {
            return null;
        }
        DecodeBudget budget = new DecodeBudget(maxBytes, parent.config().getAllocator());
        ChannelHandler[] handlers;
        BooleanSupplier complete;
        switch (coding) {
            case "zstd" -> {
                ZstdFrameTracker frames = new ZstdFrameTracker();
                handlers =
                        new ChannelHandler[] {
                            frames, new ZstdDecoder(OUTPUT_CHUNK, MAX_WINDOW_LOG), budget
                        };
                complete = frames::atFrameBoundary;
            }
            default -> {
                SingleMemberGzipDecoder gzip = new SingleMemberGzipDecoder();
                handlers = new ChannelHandler[] {gzip, budget};
                complete = gzip::isClosed;
            }
        }
        // Its own config rather than the parent's, so the budget can be its allocator.
        EmbeddedChannel channel =
                new EmbeddedChannel(parent.id(), parent.metadata().hasDisconnect());
        channel.config().setAllocator(budget.allocator());
        channel.pipeline().addLast(handlers);
        return new Decoding(channel, budget, complete);
    }

    /**
     * One gzip member, so isClosed() means the trailer has been read. Netty's default decoder
     * accepts concatenated members and so never reports an end; bytes after the first member are
     * refused rather than ignored.
     */
    private static final class SingleMemberGzipDecoder extends JdkZlibDecoder {
        SingleMemberGzipDecoder() {
            super(ZlibWrapper.GZIP, false, 0); // 0: no output cap; Netty forwards output as it goes
        }

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
                throws Exception {
            if (isClosed() && in.isReadable()) {
                throw new DecompressionException("Data after the gzip trailer");
            }
            super.decode(ctx, in, out);
        }
    }
}
