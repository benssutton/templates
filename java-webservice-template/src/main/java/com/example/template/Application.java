package com.example.template;

import io.micronaut.runtime.Micronaut;
import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;

/**
 * Application entry point.
 *
 * <p>Maps to the Python template's {@code main.py}. Where Python hand-rolls a DI
 * container in {@code core/container.py} and builds an isolated app via
 * {@code create_app(settings)}, here Micronaut's {@code ApplicationContext} is
 * the container: beans are discovered by {@code @Singleton} + constructor
 * injection, and tests get an isolated context per class via {@code @MicronautTest}
 * (the {@code create_app} isolation analogue).
 */
@OpenAPIDefinition(info = @Info(title = "Template Micronaut Service", version = "1.0.0",
    description = "A Java Micronaut service mirroring the Python webservice template"))
public class Application {
    public static void main(String[] args) {
        Micronaut.run(Application.class, args);
    }
}
