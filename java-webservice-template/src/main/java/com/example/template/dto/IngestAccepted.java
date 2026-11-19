package com.example.template.dto;

import io.micronaut.serde.annotation.Serdeable;

/** Body of a 202 from POST /data/ingest: every batch was validated and written. */
@Serdeable
public record IngestAccepted(boolean accepted) {

    public static final IngestAccepted INSTANCE = new IngestAccepted(true);
}
