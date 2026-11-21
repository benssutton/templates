package com.example.template.auth;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.PlainJWT;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.token.DefaultRolesFinder;
import io.micronaut.security.token.config.TokenConfiguration;
import org.junit.jupiter.api.Test;

/** Signature and claims are validated upstream; the factory only names the principal. */
class SubjectlessJwtAuthenticationFactoryTest {

    private final TokenConfiguration config = new TokenConfiguration() {};
    private final SubjectlessJwtAuthenticationFactory factory =
            new SubjectlessJwtAuthenticationFactory(config, new DefaultRolesFinder(config));

    private Authentication authenticate(JWTClaimsSet claims) {
        return factory.createAuthentication(new PlainJWT(claims)).orElseThrow();
    }

    @Test
    void tokenWithSubKeepsItAsThePrincipalName() {
        JWTClaimsSet claims =
                new JWTClaimsSet.Builder()
                        .subject("alice")
                        .issuer("https://issuer.example")
                        .build();
        assertThat(authenticate(claims).getName()).isEqualTo("alice");
    }

    @Test
    void tokenWithoutSubIsAnonymous() {
        JWTClaimsSet claims = new JWTClaimsSet.Builder().issuer("https://issuer.example").build();
        assertThat(authenticate(claims).getName())
                .isEqualTo(SubjectlessJwtAuthenticationFactory.ANONYMOUS);
    }
}
