package com.example.template.controller;

import com.example.template.dto.DataRowsResponse;
import com.example.template.service.DataService;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

/** Mirrors Python routers/data.py GET /data (cache + ingest added in Phase 4). */
@Controller("/data")
@ExecuteOn(TaskExecutors.BLOCKING)
public class DataController {

    private final DataService dataService;

    public DataController(DataService dataService) {
        this.dataService = dataService;
    }

    @Get
    public DataRowsResponse getData(@QueryValue(defaultValue = "10") @Min(1) @Max(100) int limit) {
        return dataService.getData(limit);
    }
}
