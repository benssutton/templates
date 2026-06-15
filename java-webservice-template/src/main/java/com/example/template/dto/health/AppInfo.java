package com.example.template.dto.health;

import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record AppInfo(String title, String version, String status) {}
