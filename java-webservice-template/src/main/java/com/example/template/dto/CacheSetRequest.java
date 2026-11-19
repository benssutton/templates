package com.example.template.dto;

import io.micronaut.core.annotation.Nullable;
import io.micronaut.json.tree.JsonNode;
import io.micronaut.serde.annotation.Serdeable;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

/**
 * {@code value} is any JSON, kept as a tree so it round-trips unchanged. {@code ttlSeconds} must be
 * positive: Redis EXPIRE with 0 or less deletes the key at once.
 */
@Serdeable
public record CacheSetRequest(
        @NotBlank String key, @Nullable JsonNode value, @Nullable @Positive Integer ttlSeconds) {}
