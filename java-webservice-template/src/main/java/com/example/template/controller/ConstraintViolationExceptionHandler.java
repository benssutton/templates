package com.example.template.controller;

import com.example.template.dto.ErrorDetail;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import io.micronaut.validation.exceptions.ConstraintExceptionHandler;
import jakarta.inject.Singleton;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import java.util.stream.Collectors;

/**
 * Bean-validation failures (body fields, {@code ?limit=}) answer 422 {@code {"detail": ...}}, as
 * FastAPI and the Rust template do, instead of Micronaut's default 400.
 */
@Produces
@Singleton
@Replaces(ConstraintExceptionHandler.class)
public class ConstraintViolationExceptionHandler
        implements ExceptionHandler<ConstraintViolationException, HttpResponse<ErrorDetail>> {

    @Override
    public HttpResponse<ErrorDetail> handle(HttpRequest request, ConstraintViolationException e) {
        String detail =
                e.getConstraintViolations().stream()
                        .map(ConstraintViolationExceptionHandler::describe)
                        .sorted()
                        .collect(Collectors.joining("; "));
        return HttpResponse.<ErrorDetail>status(HttpStatus.UNPROCESSABLE_ENTITY)
                .body(new ErrorDetail(detail));
    }

    private static String describe(ConstraintViolation<?> v) {
        String path = v.getPropertyPath().toString();
        // Drop the method prefix ("set.body.ttlSeconds" -> "ttlSeconds").
        String field = path.substring(path.lastIndexOf('.') + 1);
        return field + ": " + v.getMessage();
    }
}
