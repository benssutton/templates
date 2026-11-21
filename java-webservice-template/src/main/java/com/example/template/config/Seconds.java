package com.example.template.config;

import java.time.Duration;

final class Seconds {
    private Seconds() {}

    /** Float seconds (the env contract's unit) to a Duration, at millisecond precision. */
    static Duration of(double seconds) {
        return Duration.ofMillis(Math.round(seconds * 1000));
    }
}
