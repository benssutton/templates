package com.example.template.controller;

import com.example.template.dto.ErrorDetail;
import io.micronaut.data.connection.exceptions.ConnectionException;
import io.micronaut.data.exceptions.DataAccessException;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.server.exceptions.ExceptionHandler;
import jakarta.inject.Singleton;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import org.apache.arrow.memory.OutOfMemoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * A store that is down, unreachable or timing out is the service's problem, not the client's, and
 * is usually temporary: 503 {@code {"detail": "Service Unavailable"}}. The client timeouts in
 * application.yml and RedisFactory bound how long that takes. Any other store error (a bad query, a
 * WRONGTYPE) stays a 500.
 */
public final class StoreUnavailableHandlers {

    private static final Logger LOG = LoggerFactory.getLogger(StoreUnavailableHandlers.class);
    static final String DETAIL = "Service Unavailable";

    private StoreUnavailableHandlers() {}

    /** True when the failure, or any cause, is a connection, timeout or cancelled-query error. */
    static boolean isOutage(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof ConnectionException
                    || t instanceof JedisConnectionException
                    || t instanceof SQLTransientException
                    || t instanceof SQLRecoverableException
                    || t instanceof SQLNonTransientConnectionException
                    || t instanceof SocketTimeoutException
                    || t instanceof ConnectException) {
                return true;
            }
            // 08xxx: connection exception; 57014: query cancelled (Postgres statement_timeout).
            if (t instanceof SQLException sql
                    && sql.getSQLState() != null
                    && (sql.getSQLState().startsWith("08") || sql.getSQLState().equals("57014"))) {
                return true;
            }
        }
        return false;
    }

    static HttpResponse<ErrorDetail> respond(HttpRequest<?> request, Throwable e) {
        if (isOutage(e)) {
            LOG.error(
                    "store unavailable on {} {}: {}",
                    request.getMethod(),
                    request.getPath(),
                    e.toString());
            return HttpResponse.<ErrorDetail>status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(new ErrorDetail(DETAIL));
        }
        LOG.error("store error on {} {}", request.getMethod(), request.getPath(), e);
        return HttpResponse.<ErrorDetail>status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ErrorDetail(HttpStatus.INTERNAL_SERVER_ERROR.getReason()));
    }

    @Produces
    @Singleton
    public static class DataAccess
            implements ExceptionHandler<DataAccessException, HttpResponse<ErrorDetail>> {
        @Override
        public HttpResponse<ErrorDetail> handle(HttpRequest request, DataAccessException e) {
            return respond(request, e);
        }
    }

    @Produces
    @Singleton
    public static class Connection
            implements ExceptionHandler<ConnectionException, HttpResponse<ErrorDetail>> {
        @Override
        public HttpResponse<ErrorDetail> handle(HttpRequest request, ConnectionException e) {
            return respond(request, e);
        }
    }

    @Produces
    @Singleton
    public static class Redis
            implements ExceptionHandler<JedisConnectionException, HttpResponse<ErrorDetail>> {
        @Override
        public HttpResponse<ErrorDetail> handle(HttpRequest request, JedisConnectionException e) {
            return respond(request, e);
        }
    }

    /** The shared Arrow decode allocator is full: the client should back off and retry. */
    @Produces
    @Singleton
    public static class DecoderMemory
            implements ExceptionHandler<OutOfMemoryException, HttpResponse<ErrorDetail>> {
        @Override
        public HttpResponse<ErrorDetail> handle(HttpRequest request, OutOfMemoryException e) {
            LOG.error(
                    "ingest decoder memory exhausted on {} {}",
                    request.getMethod(),
                    request.getPath());
            return HttpResponse.<ErrorDetail>status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(new ErrorDetail(DETAIL));
        }
    }
}
