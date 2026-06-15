package com.example.template.dto.health;

import io.micronaut.serde.annotation.Serdeable;

@Serdeable
public record ProcessStats(double cpuPercent, long memoryRssBytes, int numThreads, int openFiles) {}
