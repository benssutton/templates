package com.example.template.dto.health;

import io.micronaut.core.annotation.Nullable;
import io.micronaut.serde.annotation.Serdeable;

/**
 * @param processSeconds seconds since the process started
 * @param systemBootSeconds host boot time as epoch seconds; null where the OS does not expose it
 */
@Serdeable
public record UptimeInfo(double processSeconds, @Nullable Double systemBootSeconds) {}
