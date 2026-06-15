package com.example.template.dto;

import io.micronaut.serde.annotation.Serdeable;

import java.util.List;

@Serdeable
public record DataRowsResponse(List<DataRow> rows, long total, int limit) {}
