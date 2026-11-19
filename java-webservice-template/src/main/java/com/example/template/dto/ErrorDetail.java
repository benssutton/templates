package com.example.template.dto;

import io.micronaut.serde.annotation.Serdeable;

/** Error body {@code {"detail": ...}}, the shape FastAPI and the Rust template return. */
@Serdeable
public record ErrorDetail(String detail) {}
