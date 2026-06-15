package com.example.template.dto;

import io.micronaut.serde.annotation.Serdeable;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

@Serdeable
public record ConfigSetRequest(@NotBlank String key, @NotNull String value) {}
