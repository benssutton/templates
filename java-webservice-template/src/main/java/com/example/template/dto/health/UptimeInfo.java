package com.example.template.dto.health;

import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record UptimeInfo(double processSeconds, double systemBootSeconds) {}
