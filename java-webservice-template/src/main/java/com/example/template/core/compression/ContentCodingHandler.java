package com.example.template.core.compression;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.codec.compression.ZstdDecoder;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContentDecoder;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import java.util.List;
import java.util.Locale;

/**
 * Inbound, right after the HTTP codec. Decodes zstd and gzip request bodies as they stream in, so
 * Micronaut's max-request-size counts decompressed bytes and a decompression bomb gets 413 without
 * being expanded. Bodies in any other coding pass through with their Content-Encoding intact, for
 * CompressionPolicyFilter to refuse with 415.
 *
 * <p>Also rewrites Accept-Encoding to an explicit zstd/gzip list (ContentCoding's rules: a {@code
 * *} entry counts, a missing header means identity), so the compressor after this handler
 * negotiates exactly as the other layers do. In strict mode a non-exempt request that does not
 * accept zstd loses the header, so the filter's 406 goes out uncompressed.
 */
final class ContentCodingHandler extends HttpContentDecoder {

    private final boolean requireZstd;

    ContentCodingHandler(boolean requireZstd) {
        this.requireZstd = requireZstd;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, HttpObject msg, List<Object> out)
            throws Exception {
        if (msg instanceof HttpRequest request) {
            normaliseAcceptEncoding(request);
        }
        try {
            super.decode(ctx, msg, out);
        } catch (DecoderException e) {
            // Micronaut answers a content decoder failure with 400 and closes the connection.
            DefaultLastHttpContent failed = new DefaultLastHttpContent();
            failed.setDecoderResult(DecoderResult.failure(e));
            out.add(failed);
        }
    }

    private void normaliseAcceptEncoding(HttpRequest request) {
        HttpHeaders headers = request.headers();
        ContentCoding.Weights weights =
                ContentCoding.parse(headers.getAll(HttpHeaderNames.ACCEPT_ENCODING));
        headers.remove(HttpHeaderNames.ACCEPT_ENCODING);
        if (weights == null) {
            return;
        }
        boolean refused =
                requireZstd
                        && !weights.acceptsZstd()
                        && !HttpMethod.OPTIONS.equals(request.method())
                        && !ContentCoding.isExempt(new QueryStringDecoder(request.uri()).path());
        String value = refused ? null : weights.header();
        if (value != null) {
            headers.set(HttpHeaderNames.ACCEPT_ENCODING, value);
        }
    }

    @Override
    protected EmbeddedChannel newContentDecoder(String contentEncoding) {
        ChannelHandler decoder =
                switch (contentEncoding.strip().toLowerCase(Locale.ROOT)) {
                    case "zstd" -> new ZstdDecoder();
                    case "gzip" -> ZlibCodecFactory.newZlibDecoder(ZlibWrapper.GZIP);
                    default -> null;
                };
        if (decoder == null) {
            return null;
        }
        Channel channel = ctx.channel();
        return new EmbeddedChannel(
                channel.id(), channel.metadata().hasDisconnect(), channel.config(), decoder);
    }
}
