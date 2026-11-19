package com.example.template.ingestion;

/**
 * An ingest payload that can never be stored, so retrying it is pointless: HTTP answers 4xx and the
 * Solace consumer REJECTs it (never redelivered).
 */
public abstract sealed class MalformedBatchException extends RuntimeException
        permits MalformedBatchException.Unreadable,
                MalformedBatchException.Invalid,
                MalformedBatchException.TooLarge {

    private MalformedBatchException(String message, Throwable cause) {
        super(message, cause);
    }

    /** The bytes are not an Arrow IPC stream (HTTP 400). */
    public static final class Unreadable extends MalformedBatchException {
        public Unreadable(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** A readable stream whose schema or cells cannot be merged (HTTP 422). */
    public static final class Invalid extends MalformedBatchException {
        public Invalid(String message) {
            super(message, null);
        }
    }

    /** A batch over {@code template.ingest.max-batch-bytes} (HTTP 413). */
    public static final class TooLarge extends MalformedBatchException {
        public TooLarge(String message) {
            super(message, null);
        }
    }
}
