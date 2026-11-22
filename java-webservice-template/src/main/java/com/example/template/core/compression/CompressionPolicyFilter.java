package com.example.template.core.compression;

import com.example.template.config.CompressionSettings;
import com.example.template.dto.ErrorDetail;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.order.Ordered;
import io.micronaut.http.HttpHeaders;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.RequestFilter;
import io.micronaut.http.annotation.ServerFilter;
import io.micronaut.http.filter.ServerFilterPhase;

/**
 * 415 for a request body in a coding other than zstd or gzip (ContentCodingHandler has already
 * decoded those and removed their Content-Encoding), and strict mode's 406. Runs ahead of security,
 * so a client without zstd gets 406 rather than 401.
 */
@ServerFilter("/**")
public class CompressionPolicyFilter implements Ordered {

    private final CompressionSettings settings;

    public CompressionPolicyFilter(CompressionSettings settings) {
        this.settings = settings;
    }

    @Override
    public int getOrder() {
        return ServerFilterPhase.SECURITY.before();
    }

    @RequestFilter
    @Nullable
    public HttpResponse<?> refuse(HttpRequest<?> request) {
        String encoding = request.getHeaders().get(HttpHeaders.CONTENT_ENCODING);
        if (encoding != null
                && !encoding.isBlank()
                && !encoding.strip().equalsIgnoreCase("identity")) {
            return HttpResponse.<ErrorDetail>status(HttpStatus.UNSUPPORTED_MEDIA_TYPE)
                    .header(HttpHeaders.ACCEPT_ENCODING, "zstd, gzip")
                    .contentType(MediaType.APPLICATION_JSON_TYPE)
                    .body(new ErrorDetail("Unsupported Content-Encoding"));
        }
        if (ContentCoding.strictRefuses(
                settings.requireZstd(),
                request.getMethodName(),
                ContentCoding.path(request.getUri().toString()),
                ContentCoding.parse(request.getHeaders().getAll(HttpHeaders.ACCEPT_ENCODING)))) {
            return HttpResponse.<ErrorDetail>status(HttpStatus.NOT_ACCEPTABLE)
                    .contentType(MediaType.APPLICATION_JSON_TYPE)
                    .body(new ErrorDetail("Accept-Encoding must allow zstd"));
        }
        return null; // continue the chain
    }
}
