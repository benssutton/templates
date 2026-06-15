package com.example.template.dto;

import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record CachedDataRow(long id, String name, String value, long seqno, String op) {}
