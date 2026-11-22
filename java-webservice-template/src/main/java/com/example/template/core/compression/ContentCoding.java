package com.example.template.core.compression;

import io.micronaut.core.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Content-coding rules (RFC 9110 section 12.5.3) for the two codings this service offers. A missing
 * Accept-Encoding is treated as identity only: the RFC allows any coding then, but probes and bare
 * clients that send no header cannot decode zstd.
 */
public final class ContentCoding {

    private static final Set<String> EXEMPT =
            Set.of("/health/live", "/health/ready", "/metrics", "/mcp");

    private ContentCoding() {}

    /** q-values for zstd and gzip; an explicit entry beats {@code *}. */
    public record Weights(double zstd, double gzip) {

        public boolean acceptsZstd() {
            return zstd > 0;
        }

        /** An explicit list for the compressor, or null when neither coding is acceptable. */
        @Nullable
        public String header() {
            List<String> parts = new ArrayList<>(2);
            if (zstd > 0) {
                parts.add(String.format(Locale.ROOT, "zstd;q=%.3f", zstd));
            }
            if (gzip > 0) {
                parts.add(String.format(Locale.ROOT, "gzip;q=%.3f", gzip));
            }
            return parts.isEmpty() ? null : String.join(", ", parts);
        }
    }

    /** Null when the request has no Accept-Encoding line; a later duplicate overrides. */
    @Nullable
    public static Weights parse(List<String> acceptEncoding) {
        if (acceptEncoding.isEmpty()) {
            return null;
        }
        Double zstd = null;
        Double gzip = null;
        Double star = null;
        for (String value : acceptEncoding) {
            for (String item : value.split(",")) {
                int semi = item.indexOf(';');
                String name =
                        (semi < 0 ? item : item.substring(0, semi))
                                .strip()
                                .toLowerCase(Locale.ROOT);
                String params = semi < 0 ? "" : item.substring(semi + 1);
                switch (name) {
                    case "zstd" -> zstd = q(params);
                    case "gzip" -> gzip = q(params);
                    case "*" -> star = q(params);
                    default -> {
                        // other codings are not offered
                    }
                }
            }
        }
        double fallback = star == null ? 0.0 : star;
        return new Weights(zstd == null ? fallback : zstd, gzip == null ? fallback : gzip);
    }

    public static boolean acceptsZstd(List<String> acceptEncoding) {
        Weights w = parse(acceptEncoding);
        return w != null && w.acceptsZstd();
    }

    private static double q(String params) {
        for (String param : params.split(";")) {
            int eq = param.indexOf('=');
            if (eq >= 0 && param.substring(0, eq).strip().equalsIgnoreCase("q")) {
                try {
                    double q = Double.parseDouble(param.substring(eq + 1).strip());
                    return Double.isFinite(q) ? Math.clamp(q, 0.0, 1.0) : 0.0;
                } catch (NumberFormatException e) {
                    return 0.0;
                }
            }
        }
        return 1.0;
    }

    /** Routes strict mode leaves alone: orchestrator probes, the scrape, and MCP clients. */
    public static boolean isExempt(String path) {
        return EXEMPT.contains(path) || path.startsWith("/mcp/");
    }

    /** Text-based types only; an event stream would be held back in the encoder. */
    public static boolean isCompressible(String contentType) {
        String media = contentType.split(";", 2)[0].strip().toLowerCase(Locale.ROOT);
        if (media.equals("text/event-stream")) {
            return false;
        }
        return media.startsWith("text/")
                || media.equals("application/json")
                || media.equals("application/xml")
                || media.endsWith("+json")
                || media.endsWith("+xml");
    }
}
