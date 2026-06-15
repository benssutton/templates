package com.example.template.core;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetryTest {

    @Test
    void succeedsAfterTransientFailures() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        String result = Retry.connectWithBackoff(() -> {
            if (attempts.incrementAndGet() < 3) throw new RuntimeException("boom");
            return "ok";
        }, "test", 5, 0.001, 0.01);
        assertThat(result).isEqualTo("ok");
        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    void propagatesAfterMaxAttempts() {
        AtomicInteger attempts = new AtomicInteger();
        assertThatThrownBy(() -> Retry.connectWithBackoff(() -> {
            attempts.incrementAndGet();
            throw new RuntimeException("always");
        }, "test", 3, 0.001, 0.01)).isInstanceOf(RuntimeException.class);
        assertThat(attempts.get()).isEqualTo(3);
    }
}
