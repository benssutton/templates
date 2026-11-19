package com.example.template.core;

import io.micronaut.core.propagation.ThreadPropagatedContextElement;
import org.slf4j.MDC;

/**
 * Propagates the correlation id into SLF4J MDC on whichever thread continues the request (the
 * {@code @ExecuteOn(BLOCKING)} virtual thread, health probe threads), so every log line is stamped
 * with it. Micronaut captures the prior MDC value and restores it when the context unwinds.
 */
public record MdcPropagationContext(String correlationId)
        implements ThreadPropagatedContextElement<String> {

    @Override
    public String updateThreadContext() {
        String previous = MDC.get(CorrelationId.MDC_KEY);
        MDC.put(CorrelationId.MDC_KEY, correlationId);
        return previous;
    }

    @Override
    public void restoreThreadContext(String previous) {
        if (previous == null) {
            MDC.remove(CorrelationId.MDC_KEY);
        } else {
            MDC.put(CorrelationId.MDC_KEY, previous);
        }
    }
}
