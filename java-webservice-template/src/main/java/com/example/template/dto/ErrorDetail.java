package com.example.template.dto;

import io.micronaut.serde.annotation.Serdeable;

/** Error body {@code {"detail": ...}}. */
@Serdeable
public record ErrorDetail(String detail) {}
