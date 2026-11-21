package com.example.template.controller;

import com.example.template.dto.health.DetailedStatusResponse;
import com.example.template.dto.health.LivenessResponse;
import com.example.template.dto.health.ReadinessResponse;
import com.example.template.dto.health.ReadinessStatus;
import com.example.template.service.HealthService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

/** Liveness, readiness (503 when not ready) and detailed status. */
@Controller("/health")
@Tag(name = "health")
@ExecuteOn(TaskExecutors.BLOCKING)
public class HealthController {

    private final HealthService health;

    public HealthController(HealthService health) {
        this.health = health;
    }

    @Get("/live")
    @Operation(summary = "Liveness: the process is up")
    public LivenessResponse live() {
        return health.liveness();
    }

    @Get("/ready")
    @Operation(summary = "Readiness: every dependency and ingest is up (503 otherwise)")
    public HttpResponse<ReadinessResponse> ready() {
        ReadinessResponse r = health.readiness();
        return r.status() == ReadinessStatus.READY
                ? HttpResponse.ok(r)
                : HttpResponse.status(HttpStatus.SERVICE_UNAVAILABLE).body(r);
    }

    @Get("/status")
    @Operation(summary = "Detailed status: app, uptime, dependencies, ingest, system")
    public DetailedStatusResponse status() {
        return health.detailedStatus();
    }
}
