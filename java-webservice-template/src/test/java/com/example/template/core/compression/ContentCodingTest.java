package com.example.template.core.compression;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class ContentCodingTest {

    private static ContentCoding.Weights parse(String... values) {
        return ContentCoding.parse(Arrays.asList(values));
    }

    @Test
    void missingHeaderIsNull() {
        assertThat(ContentCoding.parse(List.of())).isNull();
    }

    @Test
    void weightsTable() {
        Object[][] cases = {
            {"", 0.0, 0.0},
            {"identity, br", 0.0, 0.0},
            {"zstd", 1.0, 0.0},
            {"gzip;q=0.5", 0.0, 0.5},
            {"*", 1.0, 1.0},
            {"*;q=0.5, gzip", 0.5, 1.0},
            {"*, zstd;q=0", 0.0, 1.0},
            {"zstd;q=abc", 0.0, 0.0},
            {"zstd;q=nan", 0.0, 0.0},
            {"zstd;q=2", 1.0, 0.0},
            {"ZSTD ; Q=0.1", 0.1, 0.0},
            {"zstd;q=0, zstd", 1.0, 0.0},
            {" , zstd ,", 1.0, 0.0},
        };
        for (Object[] c : cases) {
            ContentCoding.Weights w = parse((String) c[0]);
            assertThat(w)
                    .as((String) c[0])
                    .isEqualTo(new ContentCoding.Weights((double) c[1], (double) c[2]));
        }
    }

    @Test
    void headerLinesAreCombined() {
        assertThat(parse("gzip", "zstd;q=0.3")).isEqualTo(new ContentCoding.Weights(0.3, 1.0));
    }

    @Test
    void canonicalHeaderNamesOnlyTheWinningCoding() {
        assertThat(new ContentCoding.Weights(1.0, 0.5).winningCoding()).isEqualTo("zstd");
        assertThat(new ContentCoding.Weights(1.0, 1.0).winningCoding()).isEqualTo("zstd");
        assertThat(new ContentCoding.Weights(0.5, 1.0).winningCoding()).isEqualTo("gzip");
        assertThat(new ContentCoding.Weights(0.0, 1.0).winningCoding()).isEqualTo("gzip");
        assertThat(new ContentCoding.Weights(0.0, 0.0).winningCoding()).isNull();
    }

    @Test
    void anyPositiveQValueIsAcceptableInTheHeader() {
        assertThat(parse("zstd;q=0.0001").winningCoding()).isEqualTo("zstd");
        assertThat(parse("gzip;q=0.0001").winningCoding()).isEqualTo("gzip");
        assertThat(parse("zstd;q=0.0001, gzip;q=0.0002").winningCoding()).isEqualTo("gzip");
    }

    @Test
    void strictModeRefusesOnlyNonExemptRequestsWithoutZstd() {
        ContentCoding.Weights gzip = new ContentCoding.Weights(0.0, 1.0);
        ContentCoding.Weights zstd = new ContentCoding.Weights(1.0, 0.0);
        assertThat(ContentCoding.strictRefuses(true, "GET", "/", gzip)).isTrue();
        assertThat(ContentCoding.strictRefuses(true, "GET", "/", null)).isTrue();
        assertThat(ContentCoding.strictRefuses(true, "GET", "/", zstd)).isFalse();
        assertThat(ContentCoding.strictRefuses(false, "GET", "/", gzip)).isFalse();
        assertThat(ContentCoding.strictRefuses(true, "OPTIONS", "/", gzip)).isFalse();
        assertThat(ContentCoding.strictRefuses(true, "GET", "/metrics", gzip)).isFalse();
    }

    @Test
    void pathDropsTheQueryAndDecodes() {
        assertThat(ContentCoding.path("/metrics?x=1")).isEqualTo("/metrics");
        assertThat(ContentCoding.path("/mc%70")).isEqualTo("/mcp");
    }

    @Test
    void exemptPaths() {
        for (String path :
                List.of("/health/live", "/health/ready", "/metrics", "/mcp", "/mcp/", "/mcp/x")) {
            assertThat(ContentCoding.isExempt(path)).as(path).isTrue();
        }
        for (String path : List.of("/", "/health/status", "/mcpx", "/data", "/metrics/x")) {
            assertThat(ContentCoding.isExempt(path)).as(path).isFalse();
        }
    }

    @Test
    void compressibleTypes() {
        for (String ct :
                List.of(
                        "application/json",
                        "application/json;charset=UTF-8",
                        "text/plain",
                        "application/problem+json",
                        "application/xml",
                        "image/svg+xml")) {
            assertThat(ContentCoding.isCompressible(ct)).as(ct).isTrue();
        }
        for (String ct :
                List.of(
                        "text/event-stream",
                        "application/octet-stream",
                        "application/vnd.apache.arrow.stream",
                        "image/png",
                        "")) {
            assertThat(ContentCoding.isCompressible(ct)).as(ct).isFalse();
        }
    }
}
