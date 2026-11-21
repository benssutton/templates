package com.example.template.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class RetryTest {

    @Test
    void succeedsAfterTransientFailures() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        String result =
                Retry.connectWithBackoff(
                        () -> {
                            if (attempts.incrementAndGet() < 3) throw new RuntimeException("boom");
                            return "ok";
                        },
                        "test",
                        5,
                        Duration.ofMillis(1),
                        Duration.ofMillis(10));
        assertThat(result).isEqualTo("ok");
        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    void propagatesAfterMaxAttempts() {
        AtomicInteger attempts = new AtomicInteger();
        assertThatThrownBy(() ->
                        Retry.connectWithBackoff(
                                () -> {
                                    attempts.incrementAndGet();
                                    throw new RuntimeException("always");
                                },
                                "test",
                                3,
                                Duration.ofMillis(1),
                                Duration.ofMillis(10)))
                .isInstanceOf(RuntimeException.class);
        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    void backoffDoublesUpToTheCap() {
        Duration base = Duration.ofSeconds(1);
        Duration max = Duration.ofSeconds(30);
        assertThat(Retry.backoff(base, max, 1)).isEqualTo(Duration.ofSeconds(1));
        assertThat(Retry.backoff(base, max, 2)).isEqualTo(Duration.ofSeconds(2));
        assertThat(Retry.backoff(base, max, 5)).isEqualTo(Duration.ofSeconds(16));
        assertThat(Retry.backoff(base, max, 6)).isEqualTo(max);
    }

    /** A huge base delay and attempt number saturate at the cap instead of overflowing. */
    @Test
    void backoffSaturatesWithoutOverflow() {
        Duration huge = Duration.ofDays(365L * 1_000_000);
        Duration max = Duration.ofSeconds(30);
        assertThat(Retry.backoff(huge, max, Integer.MAX_VALUE)).isEqualTo(max);
        assertThat(Retry.backoff(huge, max, 40)).isEqualTo(max);
        assertThat(Retry.backoff(Duration.ofSeconds(1), max, Integer.MAX_VALUE)).isEqualTo(max);
        assertThat(Retry.backoff(huge, huge, 2)).isEqualTo(huge);
    }
}
