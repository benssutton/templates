package com.example.template.support;

import com.example.template.config.AppSettings;
import com.example.template.config.IngestSettings;
import java.util.List;

/** Settings records for tests that build services by hand (values mirror application.yml). */
public final class TestSettings {

    private TestSettings() {}

    public static AppSettings app() {
        return app(2.0, 5, 1.0, 30.0);
    }

    public static AppSettings app(
            double healthCheckTimeoutSeconds,
            int connectMaxAttempts,
            double connectBaseDelaySeconds,
            double connectMaxDelaySeconds) {
        return new AppSettings(
                "Template Micronaut Service",
                "1.0.0",
                "",
                "running",
                "X-Request-ID",
                healthCheckTimeoutSeconds,
                0,
                "java-template",
                "Tools for this template application.",
                "localhost",
                connectMaxAttempts,
                connectBaseDelaySeconds,
                connectMaxDelaySeconds);
    }

    public static IngestSettings.Solace solace() {
        return new IngestSettings.Solace(
                "localhost",
                55555,
                "default",
                "admin",
                "admin",
                "ingest/batches",
                "java-template/ingest");
    }

    /** No watchdog and no failure shutdown. */
    public static IngestSettings ingest() {
        return ingest(0, 0, 16L * 1024 * 1024, solace());
    }

    public static IngestSettings ingest(
            long maxDisconnectSeconds,
            int maxConsecutiveFailures,
            long maxBatchBytes,
            IngestSettings.Solace solace) {
        return new IngestSettings(
                "noop",
                maxBatchBytes,
                maxDisconnectSeconds,
                maxConsecutiveFailures,
                0,
                false,
                1_000_000,
                1000,
                4,
                List.of("id"),
                256L * 1024 * 1024,
                1.0,
                60.0,
                solace);
    }
}
