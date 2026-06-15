package com.example.template.service;

import com.example.template.config.AppSettings;
import com.example.template.core.LastRequestTracker;
import com.example.template.core.SystemMetrics;
import com.example.template.dto.health.AppInfo;
import com.example.template.dto.health.CheckResult;
import com.example.template.dto.health.DetailedStatusResponse;
import com.example.template.dto.health.IngestHealth;
import com.example.template.dto.health.LivenessResponse;
import com.example.template.dto.health.ProbeResult;
import com.example.template.dto.health.ReadinessResponse;
import com.example.template.dto.health.RequestInfo;
import com.example.template.dto.health.UptimeInfo;
import com.example.template.health.DependencyHealthProbe;
import com.example.template.health.IngestHealthProvider;
import jakarta.inject.Singleton;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class HealthService {

    private final AppSettings settings;
    private final List<DependencyHealthProbe> probes;
    private final IngestHealthProvider ingestProvider;
    private final SystemMetrics systemMetrics;
    private final LastRequestTracker lastRequest;
    private final long startMillis = System.currentTimeMillis();

    public HealthService(AppSettings settings, List<DependencyHealthProbe> probes,
                         IngestHealthProvider ingestProvider, SystemMetrics systemMetrics,
                         LastRequestTracker lastRequest) {
        this.settings = settings;
        this.probes = probes;
        this.ingestProvider = ingestProvider;
        this.systemMetrics = systemMetrics;
        this.lastRequest = lastRequest;
    }

    private double uptimeSeconds() {
        return Math.round((System.currentTimeMillis() - startMillis) / 10.0) / 100.0;
    }

    public LivenessResponse liveness() {
        return new LivenessResponse("alive", uptimeSeconds());
    }

    private List<ProbeResult> gather() {
        List<ProbeResult> results = new ArrayList<>();
        for (DependencyHealthProbe p : probes) {
            try {
                results.add(p.probe());
            } catch (Exception e) {
                results.add(ProbeResult.down(p.name(), 0.0, "unavailable"));
            }
        }
        return results;
    }

    public ReadinessResponse readiness() {
        List<ProbeResult> deps = gather();
        IngestHealth ingest = ingestProvider.currentHealth();
        String ingestStatus = "connected".equals(ingest.connectionState()) ? "up" : "down";

        List<CheckResult> checks = new ArrayList<>();
        for (ProbeResult d : deps) {
            checks.add(new CheckResult(d.name(), d.status(), d.latencyMs(), null, null, null, null, null, d.error()));
        }
        checks.add(new CheckResult("ingest", ingestStatus, null, ingest.transport(), ingest.connectionState(),
            ingest.threadAlive(), ingest.lastBatchAt(), ingest.secondsSinceLastBatch(), null));

        boolean allUp = deps.stream().allMatch(d -> d.status().equals("up")) && ingestStatus.equals("up");
        return new ReadinessResponse(allUp ? "ready" : "not_ready", checks);
    }

    public DetailedStatusResponse detailedStatus() {
        List<ProbeResult> deps = gather();
        IngestHealth ingest = ingestProvider.currentHealth();
        double bootSeconds = ManagementFactory.getRuntimeMXBean().getStartTime() / 1000.0;
        return new DetailedStatusResponse(
            new AppInfo(settings.getAppTitle(), settings.getAppVersion(), settings.getStatus()),
            new UptimeInfo(uptimeSeconds(), bootSeconds),
            deps,
            ingest,
            new RequestInfo(lastRequest.lastRequestAt()),
            systemMetrics.snapshot());
    }
}
