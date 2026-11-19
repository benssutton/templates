package com.example.template.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.micronaut.serde.annotation.Serdeable;

/** Root info page. {@code MCP} keeps its upper-case key, the one exception to snake_case. */
@Serdeable
public record RootInfo(
        String title,
        String version,
        String description,
        String docs,
        @JsonProperty("MCP") String mcp) {}
