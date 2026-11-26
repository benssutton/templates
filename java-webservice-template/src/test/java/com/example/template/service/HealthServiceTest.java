package com.example.template.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.example.template.config.AppSettings;
import com.example.template.config.IngestSettings;
import com.example.template.core.CorrelationId;
import com.example.template.core.LastRequestTracker;
import com.example.template.core.MdcPropagationContext;
import com.example.template.core.SystemMetrics;
import com.example.template.dto.health.CheckResult;
import com.example.template.dto.health.IngestHealth;
import com.example.template.dto.health.ProbeResult;
import com.example.template.dto.health.ProbeStatus;
import com.example.template.dto.health.ReadinessResponse;
import com.example.template.dto.health.ReadinessStatus;
import com.example.template.health.DependencyHealthProbe;
import com.example.template.ingestion.ConnectionState;
import com.example.template.support.TestSettings;
import io.micronaut.core.propagation.PropagatedContext;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * Probes run concurrently, each bounded by {@code health-check-timeout-seconds}, so a blackholed
 * dependency yields a prompt not_ready instead of a hung readiness probe.
 */
class HealthServiceTest {

    private static final org.slf4j.Logger PROBE_LOG = LoggerFactory.getLogger("test.probe");

    /** Returns at once: up, or throwing. */
    private static DependencyHealthProbe probe(String name, boolean throwInstead) {
        return new DependencyHealthProbe() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public ProbeResult probe() {
                PROBE_LOG.info("probing {}", name);
                if (throwInstead) throw new IllegalStateException("boom");
                return ProbeResult.up(name, 0);
            }
        };
    }

    /**
     * Blocks until all {@code parties} probes sharing {@code barrier} have started (or until
     * interrupted by the timeout): it is up only if the probes really run concurrently.
     */
    private static DependencyHealthProbe barrierProbe(String name, CountDownLatch barrier) {
        return new DependencyHealthProbe() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public ProbeResult probe() {
                barrier.countDown();
                try {
                    barrier.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted", e);
                }
                return ProbeResult.up(name, 0);
            }
        };
    }

    /** Hangs until interrupted (the timeout's cancellation), like a blackholed dependency. */
    private static DependencyHealthProbe hungProbe(String name) {
        CountDownLatch never = new CountDownLatch(1);
        return new DependencyHealthProbe() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public ProbeResult probe() {
                try {
                    never.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                throw new IllegalStateException("hung probe interrupted");
            }
        };
    }

    private static DependencyHealthProbe countingProbe(String name, AtomicInteger calls) {
        return new DependencyHealthProbe() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public ProbeResult probe() {
                calls.incrementAndGet();
                return ProbeResult.up(name, 0);
            }
        };
    }

    private static HealthService service(
            double timeoutSeconds,
            double cacheTtlSeconds,
            boolean staleFailsReadiness,
            ConnectionState ingestState,
            DependencyHealthProbe... probes) {
        AppSettings defaults = TestSettings.app(timeoutSeconds, 5, 1.0, 30.0);
        AppSettings settings =
                new AppSettings(
                        defaults.appTitle(),
                        defaults.appVersion(),
                        defaults.appDescription(),
                        defaults.status(),
                        defaults.correlationIdHeader(),
                        timeoutSeconds,
                        cacheTtlSeconds,
                        defaults.mcpName(),
                        defaults.mcpInstructions(),
                        defaults.mcpAllowedHosts(),
                        5,
                        1.0,
                        30.0);
        IngestSettings d = TestSettings.ingest();
        IngestSettings ingestSettings =
                new IngestSettings(
                        d.transport(),
                        d.maxBatchBytes(),
                        d.maxDisconnectSeconds(),
                        d.maxConsecutiveFailures(),
                        d.stalenessThresholdSeconds(),
                        staleFailsReadiness,
                        d.lsmMaxRows(),
                        d.lsmFlushRows(),
                        d.lsmCompactionRuns(),
                        d.lsmKeyColumns(),
                        d.decoderMemoryBytes(),
                        d.retryBaseDelaySeconds(),
                        d.retryMaxDelaySeconds(),
                        d.solace());
        return new HealthService(
                settings,
                ingestSettings,
                List.of(probes),
                () -> new IngestHealth("noop", ingestState, true, null, null, 0, 0, true),
                new SystemMetrics(),
                new LastRequestTracker());
    }

    private static HealthService service(
            double timeoutSeconds, double cacheTtlSeconds, DependencyHealthProbe... probes) {
        return service(timeoutSeconds, cacheTtlSeconds, false, ConnectionState.CONNECTED, probes);
    }

    private static HealthService service(double timeoutSeconds, DependencyHealthProbe... probes) {
        return service(timeoutSeconds, 0, false, ConnectionState.CONNECTED, probes);
    }

    /** Readiness follows the transport's state for every transport, noop included. */
    @Test
    void ingestThatIsNotConnectedFailsReadiness() {
        assertThat(service(1.0, 0, false, ConnectionState.DOWN).readiness().status())
                .isEqualTo(ReadinessStatus.NOT_READY);
    }

    @Test
    void readinessProbesLiveAndRefreshesTheCache() {
        AtomicInteger calls = new AtomicInteger();
        HealthService health = service(1.0, 60.0, countingProbe("pg", calls));
        health.dependencies(); // 1: fills the cache
        health.dependencies(); // cached
        health.readiness(); // 2: live
        health.dependencies(); // cached (refreshed by readiness)
        assertThat(calls).hasValue(2);
    }

    /** The stub ingest is stale: it fails readiness only when stale-fails-readiness is on. */
    @Test
    void staleIngestFailsReadinessOnlyWhenConfigured() {
        assertThat(service(1.0, 0, false, ConnectionState.CONNECTED).readiness().status())
                .isEqualTo(ReadinessStatus.READY);
        assertThat(service(1.0, 0, true, ConnectionState.CONNECTED).readiness().status())
                .isEqualTo(ReadinessStatus.NOT_READY);
    }

    /*
     * No tight wall-clock bounds: under concurrent test classes a virtual thread can be scheduled
     * late. The fast probes return at once, the hung one only ends when the timeout interrupts it,
     * and the elapsed time is only checked against the hang, not against the timeout.
     */
    @Test
    void hungProbeTimesOutAsDownWithoutBlockingTheOthers() {
        HealthService svc =
                service(
                        1.5,
                        probe("postgres", false),
                        hungProbe("clickhouse"),
                        probe("redis", false));

        long start = System.nanoTime();
        ReadinessResponse r = svc.readiness();
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        assertThat(elapsed).isLessThan(Duration.ofSeconds(10));
        assertThat(r.status()).isEqualTo(ReadinessStatus.NOT_READY);
        assertThat(r.checks())
                .filteredOn(c -> c.name().equals("clickhouse"))
                .singleElement()
                .satisfies(c -> {
                    assertThat(c.status()).isEqualTo(ProbeStatus.DOWN);
                    assertThat(c.error()).isEqualTo("unavailable");
                });
        assertThat(r.checks())
                .filteredOn(c -> !c.name().equals("clickhouse") && !c.name().equals("ingest"))
                .extracting(CheckResult::status)
                .containsOnly(ProbeStatus.UP);
    }

    /** Each probe waits for the other two to start, so all are up only if they run at once. */
    @Test
    void probesRunConcurrently() {
        CountDownLatch allStarted = new CountDownLatch(3);
        HealthService svc =
                service(
                        5.0,
                        barrierProbe("a", allStarted),
                        barrierProbe("b", allStarted),
                        barrierProbe("c", allStarted));
        assertThat(svc.readiness().status()).isEqualTo(ReadinessStatus.READY);
    }

    @Test
    void throwingProbeIsDownAndKeepsOrder() {
        HealthService svc = service(1.0, probe("a", false), probe("b", true));
        assertThat(svc.detailedStatus().dependencies())
                .extracting(ProbeResult::name, ProbeResult::status)
                .containsExactly(tuple("a", ProbeStatus.UP), tuple("b", ProbeStatus.DOWN));
    }

    /**
     * Probe threads run in the request's propagated context, so their logs carry its correlation
     * id.
     */
    @Test
    void probeLogLinesCarryTheRequestCorrelationId() {
        Logger logger = (Logger) LoggerFactory.getLogger("test.probe");
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            HealthService svc = service(1.0, probe("a", false));
            try (PropagatedContext.Scope ignored =
                    PropagatedContext.getOrEmpty()
                            .plus(new MdcPropagationContext("cid-probe-1"))
                            .propagate()) {
                svc.readiness();
            }
            assertThat(appender.list)
                    .isNotEmpty()
                    .allSatisfy(e ->
                            assertThat(e.getMDCPropertyMap())
                                    .containsEntry(CorrelationId.MDC_KEY, "cid-probe-1"));
        } finally {
            logger.detachAppender(appender);
        }
    }

    /**
     * /health/status and /metrics reuse one probe round within the cache TTL; readiness is always
     * live.
     */
    @Test
    void statusReusesProbeResultsWithinTheTtl() {
        AtomicInteger calls = new AtomicInteger();
        DependencyHealthProbe counting =
                new DependencyHealthProbe() {
                    @Override
                    public String name() {
                        return "a";
                    }

                    @Override
                    public ProbeResult probe() {
                        calls.incrementAndGet();
                        return ProbeResult.up("a", 0);
                    }
                };
        HealthService svc = service(1.0, 60, counting);
        svc.detailedStatus();
        svc.detailedStatus();
        svc.dependencies();
        assertThat(calls).hasValue(1);
        svc.readiness();
        assertThat(calls).hasValue(2);
    }
}
