package com.example.template.config;

import io.micronaut.context.annotation.ConfigurationProperties;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.PositiveOrZero;

/**
 * Content coding ({@code template.compression.*}): zstd preferred, gzip fallback, negotiated from
 * Accept-Encoding. zstd and gzip request bodies are always accepted.
 *
 * @param enabled false turns response compression off (requests are still decoded)
 * @param minBytes smaller responses go uncompressed: under one TCP segment, compressing saves bytes
 *     but no round trips
 * @param zstdLevel server-side only, since high levels cost far more CPU for little gain
 * @param requireZstd 406 for clients whose Accept-Encoding does not allow zstd, except on the
 *     probe, metrics and MCP routes
 */
@ConfigurationProperties("template.compression")
public record CompressionSettings(
        boolean enabled,
        @PositiveOrZero long minBytes,
        @Min(1) @Max(19) int zstdLevel,
        boolean requireZstd) {

    public CompressionSettings {
        if (requireZstd && !enabled) {
            throw new IllegalArgumentException(
                    "template.compression.require-zstd=true requires "
                            + "template.compression.enabled=true");
        }
    }

    /** Strict mode compresses every eligible response, however small. */
    public long effectiveMinBytes() {
        return requireZstd ? 0 : minBytes;
    }
}
