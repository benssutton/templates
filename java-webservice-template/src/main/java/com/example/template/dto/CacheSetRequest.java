package com.example.template.dto;

import io.micronaut.core.annotation.Nullable;
import io.micronaut.serde.annotation.Serdeable;
import jakarta.validation.constraints.NotBlank;

@Serdeable
public record CacheSetRequest(@NotBlank String key, Object value, @Nullable Integer ttlSeconds) {}
