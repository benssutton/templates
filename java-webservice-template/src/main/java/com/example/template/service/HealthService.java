package com.example.template.service;

import com.example.template.config.AppSettings;
import com.example.template.config.IngestSettings;
import com.example.template.core.LastRequestTracker;
import com.example.template.core.SystemMetrics;
import com.example.template.dto.health.AppInfo;
import com.example.template.dto.health.CheckResult;
import com.example.template.dto.health.DetailedStatusResponse;
import com.example.template.dto.health.IngestHealth;
import com.example.template.dto.health.LivenessResponse;
import com.example.template.dto.health.ProbeResult;
import com.example.template.dto.health.ProbeStatus;
import com.example.template.dto.health.ReadinessResponse;
import com.example.template.dto.health.ReadinessStatus;
import com.example.template.dto.health.RequestInfo;
import com.example.template.dto.health.UptimeInfo;
import com.example.template.health.DependencyHealthProbe;
import com.example.template.health.IngestHealthProvider;
import com.example.template.ingestion.ConnectionState;
import io.micronaut.core.propagation.PropagatedContext;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class HealthService {

    private static final Logger LOG = LoggerFactory.getLogger(HealthService.class);

    private record Snapshot(List<ProbeResult> results, long takenAtNanos) {}

    private final AppSettings settings;
    private final IngestSettings ingestSettings;
    private final List<DependencyHealthProbe> probes;
    private final IngestHealthProvider ingestProvider;
    private final SystemMetrics systemMetrics;
    private final LastRequestTracker lastRequest;
    private Snapshot cached; // guarded by this

    public HealthService(
            AppSettings settings,
            IngestSettings ingestSettings,
            List<DependencyHealthProbe> probes,
            IngestHealthProvider ingestProvider,
            SystemMetrics systemMetrics,
            LastRequestTracker lastRequest) {
        this.settings = settings;
        this.ingestSettings = ingestSettings;
        this.probes = probes;
        this.ingestProvider = ingestProvider;
        this.systemMetrics = systemMetrics;
        this.lastRequest = lastRequest;
    }

    public LivenessResponse liveness() {
        return new LivenessResponse("alive", systemMetrics.processUptimeSeconds());
    }

    /** Names of the registered dependency probes, in probe order. */
    public List<String> dependencyNames() {
        return probes.stream().map(DependencyHealthProbe::name).toList();
    }

    /**
     * Probe results reused for {@code health-cache-ttl-seconds}, so a frequent {@code /metrics}
     * scrape or {@code /health/status} poll does not ping every store on each call. Concurrent
     * callers share one probe round.
     */
    public synchronized List<ProbeResult> dependencies() {
        Snapshot s = cached;
        if (s != null
                && System.nanoTime() - s.takenAtNanos() < settings.healthCacheTtl().toNanos()) {
            return s.results();
        }
        return probeNow();
    }

    /** Probe every dependency and make the result the cached snapshot (the only cache writer). */
    private synchronized List<ProbeResult> probeNow() {
        List<ProbeResult> results = probeAll();
        cached = new Snapshot(results, System.nanoTime());
        return results;
    }

    /**
     * Runs every probe concurrently (one virtual thread each), bounded by {@code
     * health-check-timeout-seconds}: a blackholed dependency reports down after the timeout instead
     * of hanging the readiness probe. Results keep probe order.
     */
    private List<ProbeResult> probeAll() {
        long timeoutNanos = settings.healthCheckTimeout().toNanos();
        // Not try-with-resources: close() would wait for a hung probe to finish.
        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
        // Bare threads start with an empty context: carry the request's propagated
        // context (correlation id in MDC, ServerRequestContext) into each probe.
        PropagatedContext context = PropagatedContext.getOrEmpty();
        try {
            List<Future<ProbeResult>> futures =
                    probes.stream()
                            .map(p -> pool.submit(context.wrap((Callable<ProbeResult>) p::probe)))
                            .toList();
            long deadline = System.nanoTime() + timeoutNanos;
            List<ProbeResult> results = new ArrayList<>(probes.size());
            for (int i = 0; i < probes.size(); i++) {
                results.add(await(probes.get(i), futures.get(i), deadline));
            }
            return results;
        } finally {
            pool.shutdownNow(); // interrupts any probe still running past the deadline
        }
    }

    private ProbeResult await(
            DependencyHealthProbe probe, Future<ProbeResult> future, long deadline) {
        double latencyMs = 0.0;
        try {
            return future.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (TimeoutException e) {
            latencyMs = settings.healthCheckTimeout().toMillis();
            LOG.error(
                    "health probe {} timed out after {}ms",
                    probe.name(),
                    settings.healthCheckTimeout().toMillis());
        } catch (ExecutionException e) {
            LOG.error("health probe {} failed", probe.name(), e.getCause());
        }
        future.cancel(true);
        // Never the raw exception: health payloads are unauthenticated (detail is logged).
        return ProbeResult.down(probe.name(), latencyMs, "unavailable");
    }

    /**
     * Always probes live: an orchestrator acts on readiness, so it must not be stale. The result
     * also refreshes the cache.
     */
    public ReadinessResponse readiness() {
        List<ProbeResult> deps = probeNow();
        IngestHealth ingest = ingestProvider.currentHealth();
        ProbeStatus ingestStatus =
                ingestUp(ingest, ingestSettings.staleFailsReadiness())
                        ? ProbeStatus.UP
                        : ProbeStatus.DOWN;

        List<CheckResult> checks =
                new ArrayList<>(deps.stream().map(CheckResult::dependency).toList());
        checks.add(CheckResult.ingest(ingest, ingestStatus));

        boolean allUp = deps.stream().allMatch(ProbeResult::isUp) && ingestStatus == ProbeStatus.UP;
        return new ReadinessResponse(
                allUp ? ReadinessStatus.READY : ReadinessStatus.NOT_READY, checks);
    }

    /**
     * With the noop transport there is nothing to be connected to, so ingest never blocks
     * readiness.
     */
    private static boolean ingestUp(IngestHealth ingest, boolean staleFailsReadiness) {
        if (staleFailsReadiness && ingest.stale()) {
            return false;
        }
        return "noop".equals(ingest.transport())
                || ingest.connectionState() == ConnectionState.CONNECTED;
    }

    public DetailedStatusResponse detailedStatus() {
        return new DetailedStatusResponse(
                new AppInfo(settings.appTitle(), settings.appVersion(), settings.status()),
                new UptimeInfo(
                        systemMetrics.processUptimeSeconds(),
                        systemMetrics.systemBootEpochSeconds()),
                dependencies(),
                ingestProvider.currentHealth(),
                new RequestInfo(lastRequest.lastRequestAt()),
                systemMetrics.snapshot());
    }
}
