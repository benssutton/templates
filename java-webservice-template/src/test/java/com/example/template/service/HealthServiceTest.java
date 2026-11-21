package com.example.template.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.example.template.config.AppSettings;
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
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * Probes run concurrently, each bounded by {@code health-check-timeout-seconds}, so a blackholed
 * dependency yields a prompt not_ready instead of a hung readiness probe.
 */
class HealthServiceTest {

    private static final org.slf4j.Logger PROBE_LOG = LoggerFactory.getLogger("test.probe");

    private static DependencyHealthProbe probe(String name, Duration delay, boolean throwInstead) {
        return new DependencyHealthProbe() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public ProbeResult probe() {
                PROBE_LOG.info("probing {}", name);
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (throwInstead) throw new IllegalStateException("boom");
                return ProbeResult.up(name, delay.toMillis());
            }
        };
    }

    private static HealthService service(
            double timeoutSeconds, double cacheTtlSeconds, DependencyHealthProbe... probes) {
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
        return new HealthService(
                settings,
                List.of(probes),
                () ->
                        new IngestHealth(
                                "noop", ConnectionState.CONNECTED, true, null, null, 0, 0, false),
                new SystemMetrics(),
                new LastRequestTracker());
    }

    private static HealthService service(double timeoutSeconds, DependencyHealthProbe... probes) {
        return service(timeoutSeconds, 0, probes);
    }

    @Test
    void hungProbeTimesOutAsDownWithoutBlockingTheOthers() {
        HealthService svc =
                service(
                        0.3,
                        probe("postgres", Duration.ofMillis(10), false),
                        probe("clickhouse", Duration.ofSeconds(30), false),
                        probe("redis", Duration.ofMillis(10), false));

        long start = System.nanoTime();
        ReadinessResponse r = svc.readiness();
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        assertThat(elapsed).isLessThan(Duration.ofSeconds(2));
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

    @Test
    void probesRunConcurrently() {
        HealthService svc =
                service(
                        2.0,
                        probe("a", Duration.ofMillis(400), false),
                        probe("b", Duration.ofMillis(400), false),
                        probe("c", Duration.ofMillis(400), false));
        long start = System.nanoTime();
        assertThat(svc.readiness().status()).isEqualTo(ReadinessStatus.READY);
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofMillis(1000));
    }

    @Test
    void throwingProbeIsDownAndKeepsOrder() {
        HealthService svc =
                service(1.0, probe("a", Duration.ZERO, false), probe("b", Duration.ZERO, true));
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
            HealthService svc = service(1.0, probe("a", Duration.ZERO, false));
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
