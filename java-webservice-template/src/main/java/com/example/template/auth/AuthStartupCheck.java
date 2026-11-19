package com.example.template.auth;

import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Requires;
import java.nio.charset.StandardCharsets;

/**
 * Fails startup when auth is enabled without a usable HS256 secret, rather than serving with a key
 * anyone could guess (or failing on the first request).
 */
@Context
@Requires(property = "template.auth.enabled", value = "true")
public class AuthStartupCheck {

    static final int MIN_SECRET_BYTES = 32;

    public AuthStartupCheck(AuthSettings settings) {
        if (settings.jwtSecret().getBytes(StandardCharsets.UTF_8).length < MIN_SECRET_BYTES) {
            throw new IllegalStateException(
                    "AUTH_ENABLED=true needs AUTH_JWT_SECRET of at least "
                            + MIN_SECRET_BYTES
                            + " bytes (HS256)");
        }
    }
}
