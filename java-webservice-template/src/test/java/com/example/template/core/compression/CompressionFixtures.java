package com.example.template.core.compression;

import com.github.luben.zstd.ZstdOutputStream;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.sse.Event;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;
import org.reactivestreams.Publisher;

/** Test payloads and the {@code /__compression} controller, a bean in every test context. */
final class CompressionFixtures {

    private CompressionFixtures() {}

    static String payload(int n) {
        return "{\"x\":\"" + "a".repeat(n - 8) + "\"}";
    }

    static byte[] gzip(byte[] data) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream gz = new GZIPOutputStream(out)) {
            gz.write(data);
        }
        return out.toByteArray();
    }

    /** zstd of {@code n} zero bytes, streamed so the plain bytes never exist in memory. */
    static byte[] zstdZeros(long n) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] block = new byte[1 << 20];
        try (ZstdOutputStream z = new ZstdOutputStream(out, 3)) {
            for (long left = n; left > 0; left -= block.length) {
                z.write(block, 0, (int) Math.min(left, block.length));
            }
        }
        return out.toByteArray();
    }

    /** gzip of {@code n} zero bytes, streamed like zstdZeros. */
    static byte[] gzipZeros(long n) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] block = new byte[1 << 20];
        try (GZIPOutputStream gz = new GZIPOutputStream(out)) {
            for (long left = n; left > 0; left -= block.length) {
                gz.write(block, 0, (int) Math.min(left, block.length));
            }
        }
        return out.toByteArray();
    }

    @Controller("/__compression")
    static class Payloads {
        @Get(value = "/sized/{n}", produces = MediaType.APPLICATION_JSON)
        String sized(int n) {
            return payload(n);
        }

        @Get(value = "/events", produces = MediaType.TEXT_EVENT_STREAM)
        Publisher<Event<String>> events() {
            return Publishers.just(Event.of("a".repeat(2000)));
        }

        @Get(value = "/binary", produces = MediaType.APPLICATION_OCTET_STREAM)
        byte[] binary() {
            return new byte[4096];
        }

        @Post(
                value = "/echo",
                consumes = MediaType.TEXT_PLAIN,
                produces = MediaType.APPLICATION_OCTET_STREAM)
        byte[] echo(@Body String body) {
            return body.getBytes(StandardCharsets.UTF_8);
        }
    }
}
