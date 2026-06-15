package com.example.template.core;

import io.micronaut.core.propagation.ThreadPropagatedContextElement;
import org.slf4j.MDC;

/**
 * Propagates the correlation id into SLF4J MDC on whichever thread continues the
 * request — including the {@code @ExecuteOn(BLOCKING)} virtual thread the handler
 * runs on — so every log line is stamped with it. Micronaut captures the prior
 * MDC value and restores it when the context unwinds.
 */
public record MdcPropagationContext(String correlationId) implements ThreadPropagatedContextElement<String> {

    static final String KEY = "correlationId";

    @Override
    public String updateThreadContext() {
        String previous = MDC.get(KEY);
        MDC.put(KEY, correlationId);
        return previous;
    }

    @Override
    public void restoreThreadContext(String previous) {
        if (previous == null) {
            MDC.remove(KEY);
        } else {
            MDC.put(KEY, previous);
        }
    }
}
