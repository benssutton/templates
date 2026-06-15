package com.example.template.dto;

import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record ConfigEntry(String key, String value) {}
