package com.example.template.core.compression;

import com.example.template.config.CompressionSettings;
import io.netty.handler.codec.compression.StandardCompressionOptions;
import io.netty.handler.codec.compression.ZstdOptions;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;

/**
 * Outbound: zstd at the configured level, gzip at Netty's default, chosen from the (normalised)
 * Accept-Encoding with zstd winning ties. Netty already skips HEAD, 1xx, 204, 304 and responses
 * that carry a Content-Encoding; this adds the content-type, 406 and size rules, and Vary.
 */
final class ZstdContentCompressor extends HttpContentCompressor {

    private final long minBytes;

    ZstdContentCompressor(CompressionSettings settings) {
        super(
                (int) Math.min(settings.effectiveMinBytes(), Integer.MAX_VALUE),
                zstd(settings.zstdLevel()),
                StandardCompressionOptions.gzip());
        this.minBytes = settings.effectiveMinBytes();
    }

    private static ZstdOptions zstd(int level) {
        ZstdOptions defaults = StandardCompressionOptions.zstd();
        return StandardCompressionOptions.zstd(
                level, defaults.blockSize(), defaults.maxEncodeSize());
    }

    @Override
    protected Result beginEncode(HttpResponse response, String acceptEncoding) throws Exception {
        HttpHeaders headers = response.headers();
        String type = headers.get(HttpHeaderNames.CONTENT_TYPE);
        if (type == null || !ContentCoding.isCompressible(type)) {
            return null;
        }
        if (!varies(headers)) {
            headers.add(HttpHeaderNames.VARY, HttpHeaderNames.ACCEPT_ENCODING);
        }
        if (response.status().code() == HttpResponseStatus.NOT_ACCEPTABLE.code()) {
            return null;
        }
        // A full response is measured by Netty itself; a streamed one only if it announces a
        // length.
        long length = HttpUtil.getContentLength(response, -1L);
        if (!(response instanceof HttpContent) && length >= 0 && length < minBytes) {
            return null;
        }
        return super.beginEncode(response, acceptEncoding);
    }

    private static boolean varies(HttpHeaders headers) {
        return headers.getAll(HttpHeaderNames.VARY).stream()
                .flatMap(v -> java.util.Arrays.stream(v.split(",")))
                .anyMatch(v -> v.strip().equalsIgnoreCase("accept-encoding"));
    }
}
