package com.example.template.dto.health;

import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record HostStats(double cpuPercent, long memoryTotalBytes, long memoryAvailableBytes, double memoryPercent) {}
