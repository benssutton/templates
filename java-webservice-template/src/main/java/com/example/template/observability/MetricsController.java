package com.example.template.observability;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

/**
 * Serves the Prometheus scrape at /metrics (the built-in /prometheus endpoint is disabled). Absent
 * when {@code METRICS_ENABLED=false}: /metrics is then a 404.
 */
@Requires(property = "micronaut.metrics.enabled", notEquals = "false")
@Controller("/metrics")
@ExecuteOn(TaskExecutors.BLOCKING)
@Tag(name = "observability")
public class MetricsController {

    private final PrometheusMeterRegistry registry;

    public MetricsController(PrometheusMeterRegistry registry) {
        this.registry = registry;
    }

    @Get
    @Produces(MediaType.TEXT_PLAIN)
    @Operation(summary = "Prometheus metrics in the text exposition format")
    public String scrape() {
        return registry.scrape();
    }
}
