package com.example.template.auth;

import com.example.template.dto.ErrorDetail;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpHeaders;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import io.micronaut.security.authentication.AuthorizationException;
import io.micronaut.security.authentication.DefaultAuthorizationExceptionHandler;
import jakarta.inject.Singleton;

/**
 * The contract's auth failure: 401 {@code {"detail": "Not authenticated"}} with {@code
 * WWW-Authenticate: Bearer} for a missing or invalid token (Micronaut's default handler would
 * redirect or send an empty body). An authenticated caller denied by a rule gets 403.
 */
@Produces
@Singleton
@Requires(property = "template.auth.enabled", value = "true")
@Replaces(DefaultAuthorizationExceptionHandler.class)
public class NotAuthenticatedHandler
        implements ExceptionHandler<AuthorizationException, MutableHttpResponse<ErrorDetail>> {

    @Override
    public MutableHttpResponse<ErrorDetail> handle(HttpRequest request, AuthorizationException e) {
        if (e.isForbidden()) {
            return HttpResponse.<ErrorDetail>status(HttpStatus.FORBIDDEN)
                    .body(new ErrorDetail("Forbidden"));
        }
        return HttpResponse.<ErrorDetail>status(HttpStatus.UNAUTHORIZED)
                .header(HttpHeaders.WWW_AUTHENTICATE, "Bearer")
                .body(new ErrorDetail("Not authenticated"));
    }
}
