package com.example.template.core.compression;

import io.micronaut.core.annotation.Nullable;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.TooLongFrameException;
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
 * could otherwise grow to hundreds of MB. Past the limit the rest is released, not forwarded, and
 * the request fails with 413; memory stays within the limit plus one decoder output chunk.
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

    private final boolean requireZstd;
    private final long maxRequestSize;

    // The current request's decoder state; null while the body is not being decoded.
    @Nullable private EmbeddedChannel decoding;
    @Nullable private DecodedSizeLimit limit;
    private BooleanSupplier complete = () -> true;
    private long compressedBytes;
    private boolean failed;
    private boolean dropped;

    ContentCodingHandler(boolean requireZstd, long maxRequestSize) {
        this.requireZstd = requireZstd;
        this.maxRequestSize = maxRequestSize;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, HttpObject msg, List<Object> out)
            throws Exception {
        if (msg instanceof HttpRequest request) {
            failed = false;
            decoding = null;
            limit = null;
            joinContentEncoding(request.headers());
            normaliseAcceptEncoding(request);
        } else if (failed) {
            dropped = true; // MessageToMessageDecoder releases it
            return;
        }
        try {
            DecodedSizeLimit current = limit;
            if (current != null && msg instanceof HttpContent content) {
                decodeContent(ctx, content, current, out);
            } else {
                super.decode(ctx, msg, out);
            }
        } catch (DecoderException e) {
            // Micronaut answers a content decoder failure with 400 (413 for TooLongFrameException)
            // and closes the connection.
            failed = true;
            discardDecoder();
            DefaultLastHttpContent failure = new DefaultLastHttpContent();
            failure.setDecoderResult(DecoderResult.failure(e));
            out.add(failure);
        }
    }

    private void decodeContent(
            ChannelHandlerContext ctx,
            HttpContent content,
            DecodedSizeLimit current,
            List<Object> out)
            throws Exception {
        ByteBuf bytes = content.content();
        compressedBytes += bytes.readableBytes();
        if (!(content instanceof LastHttpContent last)) {
            super.decode(ctx, content, out);
            current.check();
            return;
        }
        // Decode the final bytes before finishing, so a cut-off stream fails the request; the
        // decoders themselves accept one silently.
        super.decode(ctx, new DefaultHttpContent(bytes), out);
        current.check();
        if (compressedBytes > 0 && !complete.getAsBoolean()) {
            throw new DecompressionException("Truncated compressed request body");
        }
        decoding = null;
        limit = null;
        super.decode(ctx, last.replace(Unpooled.EMPTY_BUFFER), out);
    }

    /**
     * Closes a failed decoder now: left open, it would be finished when the next request arrives
     * and could throw again, failing that request too.
     */
    private void discardDecoder() {
        EmbeddedChannel broken = decoding;
        decoding = null;
        limit = null;
        if (broken != null) {
            try {
                broken.finishAndReleaseAll();
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
        DecodedSizeLimit sizeLimit = new DecodedSizeLimit(maxRequestSize);
        ChannelHandler[] handlers;
        switch (contentEncoding.strip().toLowerCase(Locale.ROOT)) {
            case "zstd" -> {
                ZstdFrameTracker frames = new ZstdFrameTracker();
                handlers = new ChannelHandler[] {frames, new ZstdDecoder(), sizeLimit};
                complete = frames::atFrameBoundary;
            }
            case "gzip" -> {
                SingleMemberGzipDecoder gzip = new SingleMemberGzipDecoder();
                handlers = new ChannelHandler[] {gzip, sizeLimit};
                complete = gzip::isClosed;
            }
            default -> {
                return null;
            }
        }
        limit = sizeLimit;
        compressedBytes = 0;
        Channel channel = ctx.channel();
        decoding =
                new EmbeddedChannel(
                        channel.id(),
                        channel.metadata().hasDisconnect(),
                        channel.config(),
                        handlers);
        return decoding;
    }

    /**
     * One gzip member, so isClosed() means the trailer has been read. Netty's default decoder
     * accepts concatenated members and so never reports an end; bytes after the first member are
     * refused rather than ignored.
     */
    private static final class SingleMemberGzipDecoder extends JdkZlibDecoder {
        SingleMemberGzipDecoder() {
            super(ZlibWrapper.GZIP, false, 0);
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

    /** After the decoder: forwards decoded bytes while the running total is within the limit. */
    private static final class DecodedSizeLimit extends ChannelInboundHandlerAdapter {
        private final long max;
        private long total;
        private boolean exceeded;

        DecodedSizeLimit(long max) {
            this.max = max;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ByteBuf buf = (ByteBuf) msg;
            if (!exceeded) {
                total += buf.readableBytes();
                exceeded = total > max;
            }
            if (exceeded) {
                buf.release();
            } else {
                ctx.fireChannelRead(buf);
            }
        }

        void check() {
            if (exceeded) {
                throw new TooLongFrameException(
                        "Decompressed request body exceeds " + max + " bytes");
            }
        }
    }
}
