package com.example.template.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.template.persistence.cachestore.redis.RedisSettings;
import io.micronaut.context.ApplicationContext;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Settings are validated records: a bad value stops startup instead of misbehaving later. */
class SettingsValidationTest {

    /** No stores: these contexts only bind and validate configuration. */
    private static ApplicationContext start(Map<String, Object> overrides) {
        Map<String, Object> props = new HashMap<>();
        props.put("datasources.default.enabled", "false");
        props.put("datasources.clickhouse.enabled", "false");
        props.put("flyway.enabled", "false");
        props.putAll(overrides);
        return ApplicationContext.run(props, "test");
    }

    @Test
    void defaultsBindIntoTheRecords() {
        try (ApplicationContext ctx = start(Map.of())) {
            AppSettings app = ctx.getBean(AppSettings.class);
            IngestSettings ingest = ctx.getBean(IngestSettings.class);
            assertThat(app.correlationIdHeader()).isEqualTo("X-Request-ID");
            assertThat(ingest.transport()).isEqualTo("noop");
            assertThat(ingest.maxConsecutiveFailures()).isEqualTo(5);
            assertThat(ingest.solace().port()).isEqualTo(55555);
        }
    }

    @Test
    void secondsSettingsAreExposedAsDurations() {
        try (ApplicationContext ctx =
                start(
                        Map.of(
                                "template.health-check-timeout-seconds", "2.5",
                                "template.ingest.retry-base-delay-seconds", "0.25",
                                "template.ingest.max-disconnect-seconds", "7"))) {
            assertThat(ctx.getBean(AppSettings.class).healthCheckTimeout())
                    .isEqualTo(Duration.ofMillis(2500));
            IngestSettings ingest = ctx.getBean(IngestSettings.class);
            assertThat(ingest.retryBaseDelay()).isEqualTo(Duration.ofMillis(250));
            assertThat(ingest.maxDisconnect()).isEqualTo(Duration.ofSeconds(7));
        }
    }

    @Test
    void redisTimeoutsBindAsDurations() {
        try (ApplicationContext ctx = start(Map.of("redis.connect-timeout", "1500ms"))) {
            RedisSettings redis = ctx.getBean(RedisSettings.class);
            assertThat(redis.connectTimeout()).isEqualTo(Duration.ofMillis(1500));
            assertThat(redis.socketTimeout()).isEqualTo(Duration.ofSeconds(2));
        }
    }

    @Test
    void invalidValuesFailStartup() {
        List<Map<String, Object>> invalid =
                List.of(
                        Map.of("template.ingest.lsm-max-rows", "0"),
                        Map.of("template.ingest.lsm-flush-rows", "0"),
                        Map.of("template.ingest.lsm-compaction-runs", "1"),
                        Map.of("template.ingest.lsm-key-columns", "id,name"),
                        Map.of("template.ingest.transport", "kafka"),
                        Map.of("template.ingest.solace.port", "70000"),
                        Map.of("template.health-check-timeout-seconds", "0"),
                        Map.of("template.connect-max-attempts", "0"),
                        Map.of("template.mcp-allowed-hosts", ""),
                        Map.of("template.mcp-allowed-hosts", "localhost, ,app"));
        for (Map<String, Object> bad : invalid) {
            assertThatThrownBy(() -> start(bad).close())
                    .as("%s", bad)
                    .isInstanceOf(RuntimeException.class);
        }
    }

    @Test
    void compressionDefaults() {
        try (ApplicationContext ctx = start(Map.of())) {
            CompressionSettings c = ctx.getBean(CompressionSettings.class);
            assertThat(c.enabled()).isTrue();
            assertThat(c.minBytes()).isEqualTo(1024);
            assertThat(c.zstdLevel()).isEqualTo(3);
            assertThat(c.requireZstd()).isFalse();
            assertThat(c.effectiveMinBytes()).isEqualTo(1024);
        }
    }

    @Test
    void strictModeIgnoresTheThreshold() {
        try (ApplicationContext ctx = start(Map.of("template.compression.require-zstd", "true"))) {
            assertThat(ctx.getBean(CompressionSettings.class).effectiveMinBytes()).isZero();
        }
    }

    @Test
    void zstdLevelOutside1To19FailsStartup() {
        for (String level : List.of("0", "20")) {
            assertThatThrownBy(
                            () -> {
                                try (ApplicationContext ctx =
                                        start(Map.of("template.compression.zstd-level", level))) {
                                    ctx.getBean(CompressionSettings.class);
                                }
                            })
                    .as(level)
                    .hasStackTraceContaining("zstdLevel");
        }
    }

    @Test
    void strictModeNeedsCompressionEnabled() {
        assertThatThrownBy(
                        () -> {
                            try (ApplicationContext ctx =
                                    start(
                                            Map.of(
                                                    "template.compression.enabled", "false",
                                                    "template.compression.require-zstd", "true"))) {
                                ctx.getBean(CompressionSettings.class);
                            }
                        })
                .hasStackTraceContaining("template.compression.require-zstd");
    }
}
