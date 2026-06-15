package com.example.template.dto.health;

import io.micronaut.serde.annotation.Serdeable;

import java.util.List;

@Serdeable
public record DetailedStatusResponse(AppInfo app, UptimeInfo uptime, List<ProbeResult> dependencies,
                                     IngestHealth ingest, RequestInfo requests, SystemSnapshot system) {}
