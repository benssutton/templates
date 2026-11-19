package com.example.template.controller;

import io.micronaut.context.annotation.Replaces;
import io.micronaut.core.convert.exceptions.ConversionErrorException;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ConversionErrorHandler;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import io.micronaut.http.server.exceptions.response.ErrorContext;
import io.micronaut.http.server.exceptions.response.ErrorResponseProcessor;
import jakarta.inject.Singleton;

/**
 * A value that cannot be converted ({@code ?limit=abc}) is a validation error: 422, as in Python
 * and Rust.
 */
@Produces
@Singleton
@Replaces(ConversionErrorHandler.class)
public class ConversionErrorExceptionHandler
        implements ExceptionHandler<ConversionErrorException, HttpResponse<?>> {

    private final ErrorResponseProcessor<?> processor;

    public ConversionErrorExceptionHandler(ErrorResponseProcessor<?> processor) {
        this.processor = processor;
    }

    @Override
    public HttpResponse<?> handle(HttpRequest request, ConversionErrorException e) {
        String detail =
                e.getArgument().getName() + ": " + e.getConversionError().getCause().getMessage();
        return processor.processResponse(
                ErrorContext.builder(request).cause(e).errorMessage(detail).build(),
                HttpResponse.status(HttpStatus.UNPROCESSABLE_ENTITY));
    }
}
