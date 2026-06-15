package com.example.template.config;

import io.micronaut.context.annotation.ConfigurationProperties;

/**
 * Typed application configuration (the {@code settings.py} analogue for
 * non-datasource fields). Env vars override via Micronaut property resolution.
 */
@ConfigurationProperties("template")
public class AppSettings {
    private String appTitle = "Template Micronaut Service";
    private String appVersion = "1.0.0";
    private String appDescription = "";
    private String status = "running";
    private String correlationIdHeader = "X-Request-ID";
    private double healthCheckTimeoutSeconds = 2.0;
    private long maxRequestBodyBytes = 16L * 1024 * 1024;
    private String mcpName = "java-template";
    private String mcpInstructions = "Tools for this template application.";
    private int connectMaxAttempts = 5;
    private double connectBaseDelaySeconds = 1.0;
    private double connectMaxDelaySeconds = 30.0;

    public String getAppTitle() { return appTitle; }
    public void setAppTitle(String v) { this.appTitle = v; }
    public String getAppVersion() { return appVersion; }
    public void setAppVersion(String v) { this.appVersion = v; }
    public String getAppDescription() { return appDescription; }
    public void setAppDescription(String v) { this.appDescription = v; }
    public String getStatus() { return status; }
    public void setStatus(String v) { this.status = v; }
    public String getCorrelationIdHeader() { return correlationIdHeader; }
    public void setCorrelationIdHeader(String v) { this.correlationIdHeader = v; }
    public double getHealthCheckTimeoutSeconds() { return healthCheckTimeoutSeconds; }
    public void setHealthCheckTimeoutSeconds(double v) { this.healthCheckTimeoutSeconds = v; }
    public long getMaxRequestBodyBytes() { return maxRequestBodyBytes; }
    public void setMaxRequestBodyBytes(long v) { this.maxRequestBodyBytes = v; }
    public String getMcpName() { return mcpName; }
    public void setMcpName(String v) { this.mcpName = v; }
    public String getMcpInstructions() { return mcpInstructions; }
    public void setMcpInstructions(String v) { this.mcpInstructions = v; }
    public int getConnectMaxAttempts() { return connectMaxAttempts; }
    public void setConnectMaxAttempts(int v) { this.connectMaxAttempts = v; }
    public double getConnectBaseDelaySeconds() { return connectBaseDelaySeconds; }
    public void setConnectBaseDelaySeconds(double v) { this.connectBaseDelaySeconds = v; }
    public double getConnectMaxDelaySeconds() { return connectMaxDelaySeconds; }
    public void setConnectMaxDelaySeconds(double v) { this.connectMaxDelaySeconds = v; }
}
