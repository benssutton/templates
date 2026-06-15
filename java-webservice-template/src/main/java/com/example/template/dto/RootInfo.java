package com.example.template.dto;

import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record RootInfo(String title, String version, String description, String docs, String mcp) {}
