package com.example.template.core;

import com.example.template.config.AppSettings;
import com.example.template.dto.health.ProbeResult;
import com.example.template.health.DependencyHealthProbe;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StartupSmokeTest {

    private AppSettings fastSettings() {
        AppSettings s = new AppSettings();
        s.setConnectMaxAttempts(2);
        s.setConnectBaseDelaySeconds(0.001);
        s.setConnectMaxDelaySeconds(0.005);
        return s;
    }

    record StubProbe(String name, boolean up) implements DependencyHealthProbe {
        public ProbeResult probe() {
            return up ? ProbeResult.up(name, 1.0) : ProbeResult.down(name, 1.0, "unavailable");
        }
    }

    @Test
    void passesWhenAllProbesUp() {
        StartupSmoke smoke = new StartupSmoke(
            List.of(new StubProbe("postgres", true), new StubProbe("redis", true)), fastSettings());
        assertThatCode(smoke::smokeTest).doesNotThrowAnyException();
    }

    @Test
    void abortsWhenAProbeStaysDown() {
        StartupSmoke smoke = new StartupSmoke(
            List.of(new StubProbe("postgres", true), new StubProbe("clickhouse", false)), fastSettings());
        assertThatThrownBy(smoke::smokeTest)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("clickhouse");
    }
}
