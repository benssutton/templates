package com.example.template;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.template.config.AppSettings;
import io.micronaut.context.annotation.Property;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

/**
 * The OpenAPI title and version are compile-time literals on {@link Application}; this keeps them
 * equal to the runtime settings that / and /health/status report.
 */
@Property(name = "datasources.default.enabled", value = "false")
@Property(name = "datasources.clickhouse.enabled", value = "false")
@MicronautTest(startApplication = false)
class OpenApiInfoTest {

    @Inject AppSettings settings;

    @Test
    void openApiInfoMatchesApplicationSettings() {
        var info = Application.class.getAnnotation(OpenAPIDefinition.class).info();
        assertThat(info.title()).isEqualTo(settings.appTitle());
        assertThat(info.version()).isEqualTo(settings.appVersion());
        assertThat(info.description()).isEqualTo(settings.appDescription());
    }
}
