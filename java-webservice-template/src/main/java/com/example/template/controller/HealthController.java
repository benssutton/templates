package com.example.template.controller;

import com.example.template.dto.health.DetailedStatusResponse;
import com.example.template.dto.health.LivenessResponse;
import com.example.template.dto.health.ReadinessResponse;
import com.example.template.service.HealthService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;

/** Mirrors Python routers/health.py: liveness, readiness (503 when not ready), detailed status. */
@Controller("/health")
@ExecuteOn(TaskExecutors.BLOCKING)
public class HealthController {

    private final HealthService health;

    public HealthController(HealthService health) {
        this.health = health;
    }

    @Get("/live")
    public LivenessResponse live() {
        return health.liveness();
    }

    @Get("/ready")
    public HttpResponse<ReadinessResponse> ready() {
        ReadinessResponse r = health.readiness();
        return r.status().equals("ready")
            ? HttpResponse.ok(r)
            : HttpResponse.status(HttpStatus.SERVICE_UNAVAILABLE).body(r);
    }

    @Get("/status")
    public DetailedStatusResponse status() {
        return health.detailedStatus();
    }
}
