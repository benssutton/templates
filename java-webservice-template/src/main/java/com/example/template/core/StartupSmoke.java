package com.example.template.core;

import com.example.template.config.AppSettings;
import com.example.template.dto.health.ProbeResult;
import com.example.template.health.DependencyHealthProbe;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.Environment;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.runtime.event.annotation.EventListener;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Fail-fast startup smoke-test (mirrors the Python eager smoke-test). Retries each
 * dependency probe with backoff and aborts boot if any stays down. Disabled under
 * the test environment so per-test containers are not gated by it.
 */
@Singleton
@Requires(notEnv = Environment.TEST)
public class StartupSmoke {

    private static final Logger LOG = LoggerFactory.getLogger(StartupSmoke.class);
    private final List<DependencyHealthProbe> probes;
    private final AppSettings settings;

    public StartupSmoke(List<DependencyHealthProbe> probes, AppSettings settings) {
        this.probes = probes;
        this.settings = settings;
    }

    @EventListener
    void onStartup(StartupEvent event) {
        smokeTest();
    }

    /** Smoke-test every dependency with backoff; throw (aborting startup) if any
     *  stays down after connectMaxAttempts. */
    public void smokeTest() {
        for (DependencyHealthProbe probe : probes) {
            try {
                Retry.connectWithBackoff(() -> {
                    ProbeResult r = probe.probe();
                    if (!"up".equals(r.status())) {
                        throw new IllegalStateException(probe.name() + " not ready: " + r.error());
                    }
                    return r;
                }, "smoke:" + probe.name(),
                   settings.getConnectMaxAttempts(),
                   settings.getConnectBaseDelaySeconds(),
                   settings.getConnectMaxDelaySeconds());
                LOG.info("startup smoke-test: {} is up", probe.name());
            } catch (Exception e) {
                throw new IllegalStateException("startup smoke-test failed for " + probe.name(), e);
            }
        }
    }
}
