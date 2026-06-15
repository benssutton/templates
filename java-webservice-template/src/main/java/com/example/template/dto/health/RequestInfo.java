package com.example.template.dto.health;

import io.micronaut.serde.annotation.Serdeable;

import java.time.Instant;

@Serdeable
public record RequestInfo(Instant lastRequestAt) {}
