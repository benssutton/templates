package com.example.template.dto;

import io.micronaut.core.annotation.Nullable;
import io.micronaut.json.tree.JsonNode;
import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record CacheEntry(String key, JsonNode value, @Nullable Integer ttlSeconds) {}
