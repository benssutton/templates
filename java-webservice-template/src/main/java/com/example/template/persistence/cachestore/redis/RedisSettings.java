package com.example.template.persistence.cachestore.redis;

import io.micronaut.context.annotation.ConfigurationProperties;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;

@ConfigurationProperties("redis")
public record RedisSettings(
        @NotBlank String uri,
        @Min(1) int poolSize,
        @NotNull Duration connectTimeout,
        @NotNull Duration socketTimeout) {}
