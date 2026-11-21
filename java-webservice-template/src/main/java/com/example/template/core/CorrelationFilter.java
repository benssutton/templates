package com.example.template.core;

import com.example.template.config.AppSettings;
import io.micronaut.core.order.Ordered;
import io.micronaut.core.propagation.MutablePropagatedContext;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.RequestFilter;
import io.micronaut.http.annotation.ResponseFilter;
import io.micronaut.http.annotation.ServerFilter;
import io.micronaut.http.filter.ServerFilterPhase;

/**
 * Adopts an inbound correlation id when it passes {@link CorrelationId}'s rule (otherwise generates
 * one), pushes it into the propagated context so it lands in MDC on the handler thread, and echoes
 * it on the response.
 */
@ServerFilter("/**")
public class CorrelationFilter implements Ordered {

    static final String ATTR = "correlationId";
    private final String header;

    public CorrelationFilter(AppSettings settings) {
        this.header = settings.correlationIdHeader();
    }

    /** Runs ahead of security; see ServerFilterPhase (correlation, timing, last-request, host). */
    @Override
    public int getOrder() {
        return ServerFilterPhase.FIRST.before();
    }

    @RequestFilter
    public void onRequest(HttpRequest<?> request, MutablePropagatedContext propagatedContext) {
        String cid = CorrelationId.adoptOrGenerate(request.getHeaders().get(header));
        request.setAttribute(ATTR, cid);
        propagatedContext.add(new MdcPropagationContext(cid));
    }

    @ResponseFilter
    public void onResponse(HttpRequest<?> request, MutableHttpResponse<?> response) {
        request.getAttribute(ATTR, String.class)
                .ifPresent(cid -> response.getHeaders().add(header, cid));
    }
}
