package com.example.template.core;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connect with randomised exponential backoff; mirrors Python core/retry.py. Hand-rolled rather
 * than {@code @Retryable}: the env contract's delays are float seconds ({@code 1.5}), while
 * {@code @Retryable} takes duration strings and documents {@code max-delay} as an overall cap
 * rather than the per-attempt ceiling used here.
 */
public final class Retry {

    private static final Logger LOG = LoggerFactory.getLogger(Retry.class);

    private Retry() {}

    /**
     * Calls {@code connect} with randomised exponential backoff (25% jitter). After {@code
     * maxAttempts} consecutive failures the last exception propagates.
     */
    public static <T> T connectWithBackoff(
            Callable<T> connect,
            String label,
            int maxAttempts,
            Duration baseDelay,
            Duration maxDelay)
            throws Exception {
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return connect.call();
            } catch (Exception e) {
                if (attempt == maxAttempts) {
                    LOG.error("{}: all {} connection attempts failed", label, maxAttempts);
                    throw e;
                }
                Duration delay = baseDelay.multipliedBy(1L << Math.min(attempt - 1, 30));
                if (delay.compareTo(maxDelay) > 0) {
                    delay = maxDelay;
                }
                long waitMs =
                        (long)
                                (delay.toMillis()
                                        * (1 + 0.25 * ThreadLocalRandom.current().nextDouble()));
                LOG.warn(
                        "{}: attempt {}/{} failed - retrying in {}s: {}",
                        label,
                        attempt,
                        maxAttempts,
                        String.format("%.3f", waitMs / 1000.0),
                        e.toString());
                Thread.sleep(waitMs);
            }
        }
        throw new IllegalStateException("unreachable");
    }
}
