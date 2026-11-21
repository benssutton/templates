package com.example.template.core;

import jakarta.inject.Singleton;
import java.time.Instant;

/** Holds the timestamp of the most recent request. */
@Singleton
public class LastRequestTracker {
    private volatile Instant lastRequestAt;

    public void mark() {
        this.lastRequestAt = Instant.now();
    }

    public Instant lastRequestAt() {
        return lastRequestAt;
    }
}
