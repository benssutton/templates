package com.example.template.dto;

import io.micronaut.serde.annotation.Serdeable;

import java.util.List;

@Serdeable
public record CachedDataRowsResponse(List<CachedDataRow> rows, long total, int limit) {}
