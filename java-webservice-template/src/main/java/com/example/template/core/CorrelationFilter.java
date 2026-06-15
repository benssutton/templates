package com.example.template.core;

import com.example.template.config.AppSettings;
import io.micronaut.core.propagation.MutablePropagatedContext;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.RequestFilter;
import io.micronaut.http.annotation.ResponseFilter;
import io.micronaut.http.annotation.ServerFilter;

import java.util.UUID;

/**
 * Adopts an inbound correlation id (or generates one), pushes it into the
 * propagated context so it lands in MDC on the handler thread, and echoes it on
 * the response. Mirrors the Python CorrelationIdMiddleware.
 */
@ServerFilter("/**")
public class CorrelationFilter {

    static final String ATTR = "correlationId";
    private final String header;

    public CorrelationFilter(AppSettings settings) {
        this.header = settings.getCorrelationIdHeader();
    }

    @RequestFilter
    public void onRequest(HttpRequest<?> request, MutablePropagatedContext propagatedContext) {
        String incoming = request.getHeaders().get(header);
        String cid = (incoming != null && !incoming.isBlank()) ? incoming : UUID.randomUUID().toString();
        request.setAttribute(ATTR, cid);
        propagatedContext.add(new MdcPropagationContext(cid));
    }

    @ResponseFilter
    public void onResponse(HttpRequest<?> request, MutableHttpResponse<?> response) {
        request.getAttribute(ATTR, String.class).ifPresent(cid -> response.getHeaders().add(header, cid));
    }
}
