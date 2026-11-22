package com.example.template.core.compression;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.template.support.IntegrationSupport;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStream;
import io.micronaut.context.annotation.Property;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.async.publisher.Publishers;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.sse.Event;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

/**
 * Content coding through the real server. java.net.http sends no Accept-Encoding of its own and
 * decodes nothing, so each test sees exactly the bytes and headers a client receives.
 */
@MicronautTest
@Property(name = "micronaut.server.max-request-size", value = "1024")
class CompressionTest extends IntegrationSupport {

    static String payload(int n) {
        return "{\"x\":\"" + "a".repeat(n - 8) + "\"}";
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

    @Inject EmbeddedServer server;

    private final HttpClient http = HttpClient.newHttpClient();

    private HttpResponse<byte[]> get(String path, @Nullable String accept) throws Exception {
        return send("GET", path, accept);
    }

    private HttpResponse<byte[]> send(String method, String path, @Nullable String accept)
            throws Exception {
        HttpRequest.Builder b =
                HttpRequest.newBuilder(server.getURI().resolve(path))
                        .method(method, HttpRequest.BodyPublishers.noBody());
        if (accept != null) {
            b.header("Accept-Encoding", accept);
        }
        return http.send(b.build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    HttpResponse<byte[]> post(
            String path, String contentType, @Nullable String encoding, byte[] body)
            throws Exception {
        HttpRequest.Builder b =
                HttpRequest.newBuilder(server.getURI().resolve(path))
                        .header("Content-Type", contentType)
                        .POST(HttpRequest.BodyPublishers.ofByteArray(body));
        if (encoding != null) {
            b.header("Content-Encoding", encoding);
        }
        return http.send(b.build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    static String encoding(HttpResponse<?> r) {
        return r.headers().firstValue("Content-Encoding").orElse(null);
    }

    static boolean varies(HttpResponse<?> r) {
        return r.headers().allValues("Vary").stream()
                .flatMap(v -> List.of(v.split(",")).stream())
                .anyMatch(v -> v.strip().equalsIgnoreCase("accept-encoding"));
    }

    static byte[] unzstd(byte[] data) throws IOException {
        try (ZstdInputStream in = new ZstdInputStream(new ByteArrayInputStream(data))) {
            return in.readAllBytes();
        }
    }

    static byte[] gunzip(byte[] data) throws IOException {
        try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(data))) {
            return in.readAllBytes();
        }
    }

    static byte[] gzip(byte[] data) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream gz = new GZIPOutputStream(out)) {
            gz.write(data);
        }
        return out.toByteArray();
    }

    static String utf8(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    @Test
    void zstdIsPreferredAndGzipIsTheFallback() throws Exception {
        HttpResponse<byte[]> z = get("/__compression/sized/2000", "gzip, zstd");
        assertThat(encoding(z)).isEqualTo("zstd");
        assertThat(varies(z)).isTrue();
        assertThat(utf8(unzstd(z.body()))).isEqualTo(payload(2000));

        HttpResponse<byte[]> g = get("/__compression/sized/2000", "gzip");
        assertThat(encoding(g)).isEqualTo("gzip");
        assertThat(utf8(gunzip(g.body()))).isEqualTo(payload(2000));
    }

    @Test
    void qValuesWildcardAndIdentityAreHonoured() throws Exception {
        Object[][] cases = {
            {"zstd;q=0.5, gzip", "gzip"},
            {"*", "zstd"},
            {"*, zstd;q=0", "gzip"},
            {"identity", null},
            {"br, deflate", null},
            {null, null},
        };
        for (Object[] c : cases) {
            HttpResponse<byte[]> r = get("/__compression/sized/2000", (String) c[0]);
            assertThat(encoding(r)).as(String.valueOf(c[0])).isEqualTo(c[1]);
            assertThat(varies(r)).as(String.valueOf(c[0])).isTrue();
        }
    }

    @Test
    void thresholdBoundary() throws Exception {
        assertThat(encoding(get("/__compression/sized/1023", "zstd"))).isNull();
        assertThat(encoding(get("/__compression/sized/1024", "zstd"))).isEqualTo("zstd");
    }

    @Test
    void headEventStreamAndBinaryAreNeverCompressed() throws Exception {
        assertThat(encoding(send("HEAD", "/__compression/sized/2000", "zstd"))).isNull();
        HttpResponse<byte[]> events = get("/__compression/events", "zstd");
        assertThat(encoding(events)).isNull();
        assertThat(utf8(events.body())).startsWith("data: ");
        assertThat(encoding(get("/__compression/binary", "zstd"))).isNull();
    }

    @Test
    void zstdAndGzipRequestBodiesAreDecoded() throws Exception {
        byte[] body = "hello world".repeat(20).getBytes(StandardCharsets.UTF_8);
        Object[][] cases = {
            {"zstd", Zstd.compress(body, 3)},
            {"gzip", gzip(body)},
            {"ZSTD", Zstd.compress(body, 3)},
            {"identity", body},
        };
        for (Object[] c : cases) {
            HttpResponse<byte[]> r =
                    post("/__compression/echo", "text/plain", (String) c[0], (byte[]) c[1]);
            assertThat(r.statusCode()).as((String) c[0]).isEqualTo(200);
            assertThat(r.body()).as((String) c[0]).isEqualTo(body);
        }
    }

    @Test
    void undecodableBodyIs400() throws Exception {
        HttpResponse<byte[]> r =
                post(
                        "/__compression/echo",
                        "text/plain",
                        "zstd",
                        "not zstd at all".getBytes(StandardCharsets.UTF_8));
        assertThat(r.statusCode()).isEqualTo(400);
        assertThat(utf8(r.body())).matches("\\{\"detail\":\".+\"}");
    }

    @Test
    void theBodyLimitCountsDecompressedBytes() throws Exception {
        byte[] bomb = Zstd.compress(new byte[1 << 20], 3);
        assertThat(bomb.length).isLessThan(1024);
        HttpResponse<byte[]> r = post("/__compression/echo", "text/plain", "zstd", bomb);
        assertThat(r.statusCode()).isEqualTo(413);
    }

    @Test
    void unsupportedOrStackedCodingIs415() throws Exception {
        byte[] body = "hello".getBytes(StandardCharsets.UTF_8);
        for (String coding : List.of("br", "deflate", "gzip, zstd")) {
            HttpResponse<byte[]> r = post("/__compression/echo", "text/plain", coding, body);
            assertThat(r.statusCode()).as(coding).isEqualTo(415);
            assertThat(r.headers().firstValue("Accept-Encoding")).contains("zstd, gzip");
            assertThat(utf8(r.body())).isEqualTo("{\"detail\":\"Unsupported Content-Encoding\"}");
        }
    }
}
