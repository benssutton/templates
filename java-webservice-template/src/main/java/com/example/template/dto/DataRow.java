package com.example.template.dto;

import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record DataRow(long id, String name, String value) {}
