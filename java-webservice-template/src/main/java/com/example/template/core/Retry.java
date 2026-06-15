package com.example.template.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

/** Connect with randomised exponential backoff; mirrors Python core/retry.py. */
public final class Retry {

    private static final Logger LOG = LoggerFactory.getLogger(Retry.class);

    private Retry() {}

    /** Calls {@code connect} with randomised exponential backoff (25% jitter).
     *  After {@code maxAttempts} consecutive failures the last exception propagates. */
    public static <T> T connectWithBackoff(Callable<T> connect, String label,
                                           int maxAttempts, double baseDelaySeconds, double maxDelaySeconds) throws Exception {
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return connect.call();
            } catch (Exception e) {
                if (attempt == maxAttempts) {
                    LOG.error("{}: all {} connection attempts failed", label, maxAttempts);
                    throw e;
                }
                double delay = Math.min(baseDelaySeconds * Math.pow(2, attempt - 1), maxDelaySeconds);
                double jitter = delay * 0.25 * ThreadLocalRandom.current().nextDouble();
                LOG.warn("{}: attempt {}/{} failed - retrying in {}s: {}", label, attempt, maxAttempts,
                    String.format("%.3f", delay + jitter), e.toString());
                Thread.sleep((long) ((delay + jitter) * 1000));
            }
        }
        throw new IllegalStateException("unreachable");
    }
}
