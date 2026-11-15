package com.example.template.dto;

import io.micronaut.context.DefaultApplicationContextBuilder;
import io.micronaut.context.annotation.Property;
import io.micronaut.json.JsonMapper;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

// This context-only test needs no database; disable both datasources so neither
// Hikari pool nor its startup schema-init runs (DB-backed tests provide real
// datasources via Testcontainers instead).
@Property(name = "datasources.default.enabled", value = "false")
@Property(name = "datasources.clickhouse.enabled", value = "false")
@Property(name = "template.ingest.transport", value = "noop")
@MicronautTest(startApplication = false, contextBuilder = ConfigDtoSerdeTest.NoEagerBeansContextBuilder.class)
class ConfigDtoSerdeTest {

    /**
     * Starts the context without eager (@Context / @Parallel) bean initialization: this test
     * only needs the serde JsonMapper, and its context is closed a few milliseconds after it
     * starts. In Micronaut core 5.0.2 an eager-enabled start spawns an untracked thread that
     * calls {@code getParallelBeans()}; if {@code close()} resets the bean definitions first,
     * that thread dies with an NPE, which Awaitility (catching uncaught exceptions by default)
     * then reports as the failure of whichever later test is mid-{@code await()}. Fixed
     * upstream in core 5.2.x (platform 5.2.0+, micronaut-projects/micronaut-core#13010: stop()
     * joins the discovery thread); drop this builder once the platform is upgraded.
     */
    public static final class NoEagerBeansContextBuilder extends DefaultApplicationContextBuilder {
        public NoEagerBeansContextBuilder() {
            eagerBeansEnabled(false);
        }
    }

    @Inject
    JsonMapper json;

    @Test
    void configEntryRoundTrips() throws Exception {
        ConfigEntry entry = new ConfigEntry("featureX", "on");
        String encoded = new String(json.writeValueAsBytes(entry));
        assertThat(encoded).contains("\"key\":\"featureX\"").contains("\"value\":\"on\"");
        ConfigEntry decoded = json.readValue(encoded, ConfigEntry.class);
        assertThat(decoded).isEqualTo(entry);
    }

    @Test
    void configSetRequestDecodes() throws Exception {
        ConfigSetRequest req = json.readValue("{\"key\":\"k\",\"value\":\"v\"}", ConfigSetRequest.class);
        assertThat(req.key()).isEqualTo("k");
        assertThat(req.value()).isEqualTo("v");
    }
}
