package com.example.template.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.template.support.IntegrationSupport;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import io.micronaut.context.ApplicationContext;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** The bearer-JWT seam switched on: every route but the probes needs a valid token. */
@MicronautTest
class AuthTest extends IntegrationSupport {

    static final String SECRET = "test-secret-that-is-at-least-32-bytes-long!";
    static final String ISSUER = "https://issuer.example";
    static final String AUDIENCE = "java-template";

    @Inject
    @Client("/")
    HttpClient client;

    @Override
    public Map<String, String> getProperties() {
        Map<String, String> props = super.getProperties();
        props.put("template.auth.enabled", "true");
        props.put("template.auth.jwt-secret", SECRET);
        props.put("micronaut.security.token.jwt.claims-validators.issuer", ISSUER);
        props.put("micronaut.security.token.jwt.claims-validators.audience", AUDIENCE);
        return props;
    }

    static String token(String secret, String issuer, String audience, Instant expires)
            throws Exception {
        return token(secret, "tester", issuer, audience, expires);
    }

    static String token(
            String secret, String subject, String issuer, String audience, Instant expires)
            throws Exception {
        JWTClaimsSet claims =
                new JWTClaimsSet.Builder()
                        .subject(subject)
                        .issuer(issuer)
                        .audience(audience)
                        .issueTime(new Date())
                        .expirationTime(Date.from(expires))
                        .build();
        SignedJWT jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256), claims);
        jwt.sign(new MACSigner(secret));
        return jwt.serialize();
    }

    static String validToken() throws Exception {
        return token(SECRET, ISSUER, AUDIENCE, Instant.now().plusSeconds(300));
    }

    record Res(int status, String body, String wwwAuthenticate) {}

    private Res send(MutableHttpRequest<?> request) {
        try {
            HttpResponse<String> r = client.toBlocking().exchange(request, String.class);
            return new Res(
                    r.code(), r.getBody().orElse(""), r.getHeaders().get("WWW-Authenticate"));
        } catch (HttpClientResponseException e) {
            return new Res(
                    e.code(),
                    e.getResponse().getBody(String.class).orElse(""),
                    e.getResponse().getHeaders().get("WWW-Authenticate"));
        }
    }

    private static void assertNotAuthenticated(Res res) {
        assertThat(res.status()).as(res.body()).isEqualTo(401);
        assertThat(res.body()).isEqualTo("{\"detail\":\"Not authenticated\"}");
        assertThat(res.wwwAuthenticate()).isEqualTo("Bearer");
    }

    @Test
    void validTokenIsAccepted() throws Exception {
        assertThat(send(HttpRequest.GET("/config").bearerAuth(validToken())).status())
                .isEqualTo(200);
    }

    @Test
    void missingTokenIs401() {
        assertNotAuthenticated(send(HttpRequest.GET("/config")));
        assertNotAuthenticated(send(HttpRequest.GET("/health/status")));
    }

    @Test
    void badSignatureIs401() throws Exception {
        String forged =
                token(
                        "another-secret-that-is-also-32-bytes-long",
                        ISSUER,
                        AUDIENCE,
                        Instant.now().plusSeconds(300));
        assertNotAuthenticated(send(HttpRequest.GET("/config").bearerAuth(forged)));
    }

    @Test
    void expiredTokenIs401() throws Exception {
        String expired = token(SECRET, ISSUER, AUDIENCE, Instant.now().minusSeconds(120));
        assertNotAuthenticated(send(HttpRequest.GET("/config").bearerAuth(expired)));
    }

    @Test
    void wrongAudienceIs401() throws Exception {
        String token = token(SECRET, ISSUER, "someone-else", Instant.now().plusSeconds(300));
        assertNotAuthenticated(send(HttpRequest.GET("/config").bearerAuth(token)));
    }

    @Test
    void wrongIssuerIs401() throws Exception {
        String token =
                token(SECRET, "https://evil.example", AUDIENCE, Instant.now().plusSeconds(300));
        assertNotAuthenticated(send(HttpRequest.GET("/config").bearerAuth(token)));
    }

    /** Python and Rust accept a token with no sub, so Java must too. */
    @Test
    void tokenWithoutSubIsAccepted() throws Exception {
        String token = token(SECRET, null, ISSUER, AUDIENCE, Instant.now().plusSeconds(300));
        assertThat(send(HttpRequest.GET("/config").bearerAuth(token)).status()).isEqualTo(200);
    }

    /** Accepting a missing sub must not skip the other claim checks. */
    @Test
    void tokenWithoutSubAndWrongIssuerIs401() throws Exception {
        String token =
                token(
                        SECRET,
                        null,
                        "https://evil.example",
                        AUDIENCE,
                        Instant.now().plusSeconds(300));
        assertNotAuthenticated(send(HttpRequest.GET("/config").bearerAuth(token)));
    }

    @Test
    void probesStayOpen() {
        assertThat(send(HttpRequest.GET("/health/live")).status()).isEqualTo(200);
        assertThat(send(HttpRequest.GET("/health/ready")).status()).isEqualTo(200);
    }

    @Test
    void mcpNeedsAToken() throws Exception {
        var ping = Map.of("jsonrpc", "2.0", "id", 1, "method", "ping");
        String accept = "application/json, text/event-stream";
        assertNotAuthenticated(
                send(
                        HttpRequest.POST("/mcp", ping)
                                .contentType(MediaType.APPLICATION_JSON)
                                .header("Accept", accept)));
        assertThat(
                        send(HttpRequest.POST("/mcp", ping)
                                        .contentType(MediaType.APPLICATION_JSON)
                                        .header("Accept", accept)
                                        .bearerAuth(validToken()))
                                .status())
                .isEqualTo(200);
    }

    /** Enabled without a usable secret is a startup failure, not an open or broken service. */
    @Test
    void enabledWithoutSecretFailsStartup() {
        Map<String, Object> props = new HashMap<>(super.getProperties());
        props.put("template.auth.enabled", "true");
        props.put("template.auth.jwt-secret", "");
        assertThatThrownBy(() -> ApplicationContext.run(props, "test").close())
                .hasStackTraceContaining("AUTH_JWT_SECRET");
    }
}
