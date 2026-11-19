package com.example.template.auth;

import io.micronaut.context.annotation.ConfigurationProperties;
import jakarta.validation.constraints.NotNull;

/**
 * The bearer-JWT seam ({@code template.auth.*}), off by default. Env: {@code AUTH_ENABLED}, {@code
 * AUTH_JWT_SECRET} (HS256 key, at least 32 bytes), {@code AUTH_JWT_ISSUER} and {@code
 * AUTH_JWT_AUDIENCE} (each checked only when set).
 */
@ConfigurationProperties("template.auth")
public record AuthSettings(
        boolean enabled,
        @NotNull String jwtSecret,
        @NotNull String jwtIssuer,
        @NotNull String jwtAudience) {}
