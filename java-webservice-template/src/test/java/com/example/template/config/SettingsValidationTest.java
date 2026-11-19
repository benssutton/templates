package com.example.template.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.micronaut.context.ApplicationContext;
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
    void invalidValuesFailStartup() {
        List<Map<String, Object>> invalid =
                List.of(
                        Map.of("template.ingest.lsm-max-rows", "0"),
                        Map.of("template.ingest.transport", "kafka"),
                        Map.of("template.ingest.solace.port", "70000"),
                        Map.of("template.health-check-timeout-seconds", "0"),
                        Map.of("template.connect-max-attempts", "0"));
        for (Map<String, Object> bad : invalid) {
            assertThatThrownBy(() -> start(bad).close())
                    .as("%s", bad)
                    .isInstanceOf(RuntimeException.class);
        }
    }
}
