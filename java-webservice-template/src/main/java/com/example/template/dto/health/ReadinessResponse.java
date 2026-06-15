package com.example.template.dto.health;

import io.micronaut.serde.annotation.Serdeable;

import java.util.List;

@Serdeable
public record ReadinessResponse(String status, List<CheckResult> checks) {}
