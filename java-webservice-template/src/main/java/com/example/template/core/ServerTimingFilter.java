package com.example.template.core;

import io.micronaut.core.order.Ordered;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.RequestFilter;
import io.micronaut.http.annotation.ResponseFilter;
import io.micronaut.http.annotation.ServerFilter;
import io.micronaut.http.filter.ServerFilterPhase;

/**
 * Installs a request-scoped {@link BoundarySamples} holder and renders the W3C {@code
 * Server-Timing} response header from the boundaries recorded during the request.
 */
@ServerFilter("/**")
public class ServerTimingFilter implements Ordered {

    private static final String START_ATTR = "serverTimingStart";

    /** Runs ahead of security; see ServerFilterPhase (correlation, timing, last-request, host). */
    @Override
    public int getOrder() {
        return ServerFilterPhase.FIRST.order();
    }

    @RequestFilter
    public void onRequest(HttpRequest<?> request) {
        request.setAttribute(BoundarySamples.ATTR, new BoundarySamples());
        request.setAttribute(START_ATTR, System.nanoTime());
    }

    @ResponseFilter
    public void onResponse(HttpRequest<?> request, MutableHttpResponse<?> response) {
        Object holder = request.getAttribute(BoundarySamples.ATTR).orElse(null);
        Long start = request.getAttribute(START_ATTR, Long.class).orElse(null);
        if (holder instanceof BoundarySamples samples && start != null) {
            double totalMs = (System.nanoTime() - start) / 1_000_000.0;
            response.getHeaders().add("Server-Timing", samples.render(totalMs));
        }
    }
}
