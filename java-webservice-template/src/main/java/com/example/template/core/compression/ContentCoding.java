package com.example.template.core.compression;

import io.micronaut.core.annotation.Nullable;
import io.netty.handler.codec.http.QueryStringDecoder;
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

        /**
         * The one coding to answer with, as an explicit Accept-Encoding value, or null when neither
         * is acceptable. Only the winner is listed because Netty's compressor takes zstd whenever
         * it is listed, ignoring q-values; zstd wins ties. It carries no q: any q above 0 is
         * acceptable, and a rounded one could print as q=0.
         */
        @Nullable
        public String winningCoding() {
            if (zstd > 0 && zstd >= gzip) {
                return "zstd";
            }
            return gzip > 0 ? "gzip" : null;
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

    /** Whether strict mode governs a request: anything but a preflight or an exempt route. */
    public static boolean strictApplies(boolean requireZstd, String method, String path) {
        return requireZstd && !method.equals("OPTIONS") && !isExempt(path);
    }

    /** Strict mode's refusal: a governed request needs an Accept-Encoding that allows zstd. */
    public static boolean strictRefuses(
            boolean requireZstd, String method, String path, @Nullable Weights weights) {
        return strictApplies(requireZstd, method, path)
                && (weights == null || !weights.acceptsZstd());
    }

    /** The decoded path of a request target, computed one way for every caller. */
    public static String path(String requestTarget) {
        return new QueryStringDecoder(requestTarget).path();
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

    /**
     * Routes strict mode leaves alone: orchestrator probes, the scrape, and MCP clients. One
     * trailing slash is ignored, so {@code /health/live/} is exempt too.
     */
    public static boolean isExempt(String path) {
        if (path.length() > 1 && path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
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
