package com.example.template;

import io.micronaut.runtime.Micronaut;

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
public class Application {
    public static void main(String[] args) {
        Micronaut.run(Application.class, args);
    }
}
