package com.example.template.observability;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;

/** Serves the Prometheus scrape at /metrics (mirrors the Python /metrics endpoint). */
@Controller("/metrics")
@ExecuteOn(TaskExecutors.BLOCKING)
public class MetricsController {

    private final PrometheusMeterRegistry registry;
    private final MetricsBinder binder;

    public MetricsController(PrometheusMeterRegistry registry, MetricsBinder binder) {
        this.registry = registry;
        this.binder = binder;
    }

    @Get
    @Produces(MediaType.TEXT_PLAIN)
    public String scrape() {
        binder.refresh();
        return registry.scrape();
    }
}
