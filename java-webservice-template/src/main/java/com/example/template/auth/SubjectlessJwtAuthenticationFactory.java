package com.example.template.auth;

import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import io.micronaut.security.token.Claims;
import io.micronaut.security.token.RolesFinder;
import io.micronaut.security.token.config.TokenConfiguration;
import io.micronaut.security.token.jwt.validator.DefaultJwtAuthenticationFactory;
import jakarta.inject.Singleton;
import java.util.Optional;

/**
 * Python and Rust accept a validly signed token with no {@code sub}; Micronaut's default factory
 * would turn it into "no authentication" (401). A missing subject becomes {@value #ANONYMOUS}.
 */
@Singleton
@Replaces(DefaultJwtAuthenticationFactory.class)
@Requires(property = "template.auth.enabled", value = "true")
public class SubjectlessJwtAuthenticationFactory extends DefaultJwtAuthenticationFactory {

    static final String ANONYMOUS = "anonymous";

    public SubjectlessJwtAuthenticationFactory(
            TokenConfiguration tokenConfiguration, RolesFinder rolesFinder) {
        super(tokenConfiguration, rolesFinder);
    }

    @Override
    protected Optional<String> usernameForClaims(Claims claims) {
        return Optional.of(super.usernameForClaims(claims).orElse(ANONYMOUS));
    }
}
