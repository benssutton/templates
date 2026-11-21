package com.example.template.core;

import io.micronaut.core.order.Ordered;
import io.micronaut.http.annotation.RequestFilter;
import io.micronaut.http.annotation.ServerFilter;
import io.micronaut.http.filter.ServerFilterPhase;

/** Records the time of each inbound request (mirrors the Python _track_last_request middleware). */
@ServerFilter("/**")
public class LastRequestFilter implements Ordered {

    private final LastRequestTracker tracker;

    public LastRequestFilter(LastRequestTracker tracker) {
        this.tracker = tracker;
    }

    /** Runs ahead of security; see ServerFilterPhase (correlation, timing, last-request, host). */
    @Override
    public int getOrder() {
        return ServerFilterPhase.FIRST.after();
    }

    @RequestFilter
    public void onRequest() {
        tracker.mark();
    }
}
