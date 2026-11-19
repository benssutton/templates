package com.example.template.auth;

import io.micronaut.context.annotation.Requires;
import io.micronaut.security.token.Claims;
import io.micronaut.security.token.jwt.validator.GenericJwtClaimsValidator;
import jakarta.inject.Singleton;
import java.util.Collection;

/**
 * Checks {@code iss} and {@code aud} against AUTH_JWT_ISSUER / AUTH_JWT_AUDIENCE, each only when
 * configured (signature and expiry are checked by micronaut-security-jwt).
 */
@Singleton
@Requires(property = "template.auth.enabled", value = "true")
public class IssuerAudienceValidator<T> implements GenericJwtClaimsValidator<T> {

    private final AuthSettings settings;

    public IssuerAudienceValidator(AuthSettings settings) {
        this.settings = settings;
    }

    @Override
    public boolean validate(Claims claims, T request) {
        String issuer = settings.jwtIssuer();
        if (!issuer.isBlank() && !issuer.equals(claims.get(Claims.ISSUER))) {
            return false;
        }
        String audience = settings.jwtAudience();
        if (audience.isBlank()) {
            return true;
        }
        Object aud = claims.get(Claims.AUDIENCE);
        return aud instanceof Collection<?> list ? list.contains(audience) : audience.equals(aud);
    }
}
