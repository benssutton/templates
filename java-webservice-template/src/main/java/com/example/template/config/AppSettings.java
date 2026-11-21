package com.example.template.config;

import io.micronaut.context.annotation.ConfigurationProperties;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import java.time.Duration;
import java.util.List;

/**
 * Typed, immutable application configuration ({@code template.*}). Defaults live in
 * application.yml; env vars override them through Micronaut property resolution. A value that
 * breaks a constraint fails startup instead of misbehaving at runtime.
 *
 * @param healthCacheTtlSeconds how long {@code /health/status} and {@code /metrics} reuse one round
 *     of dependency probes (0 probes on every call)
 * @param mcpAllowedHosts {@code /mcp} Host/Origin allow-list of {@code host[:port]} entries (the
 *     env var may be comma-separated or a JSON array, see {@code EnvContract}; parsed by {@code
 *     McpHostFilter})
 */
@ConfigurationProperties("template")
public record AppSettings(
        @NotBlank String appTitle,
        @NotBlank String appVersion,
        @NotNull String appDescription,
        @NotBlank String status,
        @NotBlank String correlationIdHeader,
        @Positive double healthCheckTimeoutSeconds,
        @PositiveOrZero double healthCacheTtlSeconds,
        @NotBlank String mcpName,
        @NotNull String mcpInstructions,
        @NotEmpty List<@NotBlank String> mcpAllowedHosts,
        @Min(1) int connectMaxAttempts,
        @Positive double connectBaseDelaySeconds,
        @Positive double connectMaxDelaySeconds) {

    // The env vars are float seconds (HEALTH_CHECK_TIMEOUT_SECONDS=2.5), which Micronaut's
    // Duration converter cannot bind; code uses these accessors only.

    public Duration healthCheckTimeout() {
        return Seconds.of(healthCheckTimeoutSeconds);
    }

    public Duration healthCacheTtl() {
        return Seconds.of(healthCacheTtlSeconds);
    }

    public Duration connectBaseDelay() {
        return Seconds.of(connectBaseDelaySeconds);
    }

    public Duration connectMaxDelay() {
        return Seconds.of(connectMaxDelaySeconds);
    }
}
