package com.example.template.dto;

import io.micronaut.context.annotation.Property;
import io.micronaut.json.JsonMapper;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

// This context-only test needs no database; disable the eager datasource so the
// Hikari pool isn't created (DB-backed tests provide a real datasource instead).
@Property(name = "datasources.default.enabled", value = "false")
@MicronautTest(startApplication = false)
class ConfigDtoSerdeTest {

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
