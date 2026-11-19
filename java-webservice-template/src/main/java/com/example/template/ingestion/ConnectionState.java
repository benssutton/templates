package com.example.template.ingestion;

import com.fasterxml.jackson.annotation.JsonValue;
import io.micronaut.serde.annotation.Serdeable;
import java.util.Locale;

/** Transport connection state, serialised lowercase ({@code "connected"}, ...). */
@Serdeable
public enum ConnectionState {
    CONNECTED,
    RECONNECTING,
    DOWN;

    @JsonValue
    public String value() {
        return name().toLowerCase(Locale.ROOT);
    }
}
