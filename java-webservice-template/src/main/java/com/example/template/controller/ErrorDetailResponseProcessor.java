package com.example.template.controller;

import com.example.template.dto.ErrorDetail;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.server.exceptions.response.Error;
import io.micronaut.http.server.exceptions.response.ErrorContext;
import io.micronaut.http.server.exceptions.response.ErrorResponseProcessor;
import jakarta.inject.Singleton;
import java.util.stream.Collectors;

/**
 * The one error body: every error Micronaut renders (unknown route 404, 405, 415, unreadable JSON
 * 400, 413, conversion 422, unhandled 500, ...) becomes {@code application/json {"detail": "..."}},
 * the shape the Python and Rust templates return. Exception handlers only choose the status. A 5xx
 * never carries the exception message, since it can leak internals; the reason phrase is used
 * instead.
 */
@Singleton
@Replaces(ErrorResponseProcessor.class)
public class ErrorDetailResponseProcessor implements ErrorResponseProcessor<ErrorDetail> {

    @Override
    public MutableHttpResponse<ErrorDetail> processResponse(
            ErrorContext context, MutableHttpResponse<?> response) {
        HttpStatus status = response.getStatus();
        String detail =
                status.getCode() >= 500 || !context.hasErrors()
                        ? status.getReason()
                        : context.getErrors().stream()
                                .map(Error::getMessage)
                                .collect(Collectors.joining("; "));
        return response.contentType(MediaType.APPLICATION_JSON_TYPE).body(new ErrorDetail(detail));
    }
}
