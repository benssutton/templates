package com.example.template.dto.health;

import com.fasterxml.jackson.annotation.JsonValue;
import io.micronaut.serde.annotation.Serdeable;
import java.util.Locale;

/** Overall readiness, serialised as {@code "ready"} / {@code "not_ready"}. */
@Serdeable
public enum ReadinessStatus {
    READY,
    NOT_READY;

    @JsonValue
    public String value() {
        return name().toLowerCase(Locale.ROOT);
    }
}
