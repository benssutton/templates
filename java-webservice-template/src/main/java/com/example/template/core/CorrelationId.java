package com.example.template.core;

import io.micronaut.core.annotation.Nullable;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * The correlation id rule shared by HTTP ({@code X-Request-ID}) and the Solace consumer: an inbound
 * id is adopted only if it is 1-128 chars of {@code [A-Za-z0-9._-]}, so a caller cannot inject log
 * lines, header splits or unbounded strings; otherwise a new UUID is generated.
 */
public final class CorrelationId {

    /** MDC key every log line is stamped with. */
    public static final String MDC_KEY = "correlationId";

    private static final Pattern VALID = Pattern.compile("[A-Za-z0-9._-]{1,128}");

    private CorrelationId() {}

    public static String adoptOrGenerate(@Nullable String inbound) {
        return inbound != null && VALID.matcher(inbound).matches()
                ? inbound
                : UUID.randomUUID().toString();
    }
}
