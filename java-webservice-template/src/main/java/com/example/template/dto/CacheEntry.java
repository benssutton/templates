package com.example.template.dto;

import io.micronaut.core.annotation.Nullable;
import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record CacheEntry(String key, Object value, @Nullable Integer ttlSeconds) {}
