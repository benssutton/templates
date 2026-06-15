package com.example.template.core;

import io.micronaut.http.annotation.RequestFilter;
import io.micronaut.http.annotation.ServerFilter;

/** Records the time of each inbound request (mirrors the Python _track_last_request middleware). */
@ServerFilter("/**")
public class LastRequestFilter {

    private final LastRequestTracker tracker;

    public LastRequestFilter(LastRequestTracker tracker) {
        this.tracker = tracker;
    }

    @RequestFilter
    public void onRequest() {
        tracker.mark();
    }
}
