package com.example.template.dto.health;

import io.micronaut.serde.annotation.Serdeable;
import java.util.List;

@Serdeable
public record ReadinessResponse(ReadinessStatus status, List<CheckResult> checks) {}
