package com.example.template.dto.health;

import com.fasterxml.jackson.annotation.JsonValue;
import io.micronaut.serde.annotation.Serdeable;
import java.util.Locale;

/** A dependency check's outcome, serialised as {@code "up"} / {@code "down"}. */
@Serdeable
public enum ProbeStatus {
    UP,
    DOWN;

    @JsonValue
    public String value() {
        return name().toLowerCase(Locale.ROOT);
    }
}
