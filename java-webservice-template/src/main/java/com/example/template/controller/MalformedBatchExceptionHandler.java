package com.example.template.controller;

import com.example.template.dto.ErrorDetail;
import com.example.template.ingestion.MalformedBatchException;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;

/**
 * Bad ingest input is the client's fault: 400 for bytes that are not Arrow IPC, 422 for a readable
 * batch that cannot be merged, 413 for a batch over {@code max-batch-bytes}.
 */
@Produces
@Singleton
public class MalformedBatchExceptionHandler
        implements ExceptionHandler<MalformedBatchException, HttpResponse<ErrorDetail>> {

    @Override
    public HttpResponse<ErrorDetail> handle(HttpRequest request, MalformedBatchException e) {
        HttpStatus status =
                switch (e) {
                    case MalformedBatchException.Unreadable u -> HttpStatus.BAD_REQUEST;
                    case MalformedBatchException.Invalid i -> HttpStatus.UNPROCESSABLE_ENTITY;
                    case MalformedBatchException.TooLarge t -> HttpStatus.REQUEST_ENTITY_TOO_LARGE;
                };
        return HttpResponse.<ErrorDetail>status(status).body(new ErrorDetail(e.getMessage()));
    }
}
