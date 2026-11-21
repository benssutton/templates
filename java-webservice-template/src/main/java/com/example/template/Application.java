package com.example.template;

import io.micronaut.runtime.Micronaut;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;

/**
 * Application entry point. Micronaut's {@code ApplicationContext} is the DI container: beans are
 * discovered by {@code @Singleton} plus constructor injection, and each test class gets an isolated
 * context via {@code @MicronautTest}.
 */
// Must match template.app-title/app-version in application.yml (the processor cannot read
// it at compile time); OpenApiInfoTest fails when they drift.
@OpenAPIDefinition(
        info =
                @Info(
                        title = "Template Micronaut Service",
                        version = "1.0.0",
                        description = "A Java Micronaut service template"))
public class Application {
    public static void main(String[] args) {
        Micronaut.run(Application.class, args);
    }
}
