package com.example.template.controller;

import com.example.template.core.Timed;
import com.example.template.dto.CachedDataRow;
import com.example.template.dto.CachedDataRowsResponse;
import com.example.template.dto.DataRowsResponse;
import com.example.template.ingestion.ArrowDecoder;
import com.example.template.persistence.streamstore.LsmRow;
import com.example.template.persistence.streamstore.LsmStore;
import com.example.template.service.DataService;
import com.example.template.service.StreamIngestService;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

import java.util.List;

/** Mirrors Python routers/data.py: GET /data (ClickHouse), GET /data/cache (LSM), POST /data/ingest. */
@Controller("/data")
@ExecuteOn(TaskExecutors.BLOCKING)
public class DataController {

    private final DataService dataService;
    private final LsmStore lsmStore;
    private final StreamIngestService ingestService;

    public DataController(DataService dataService, LsmStore lsmStore, StreamIngestService ingestService) {
        this.dataService = dataService;
        this.lsmStore = lsmStore;
        this.ingestService = ingestService;
    }

    @Get
    public DataRowsResponse getData(@QueryValue(defaultValue = "10") @Min(1) @Max(100) int limit) {
        return dataService.getData(limit);
    }

    @Get("/cache")
    public CachedDataRowsResponse getCachedData(@QueryValue(defaultValue = "10") @Min(1) @Max(100) int limit) {
        LsmStore.QueryResult result = lsmStore.query(limit);
        List<CachedDataRow> rows = result.rows().stream()
            .map(r -> new CachedDataRow(r.id(), r.name(), r.value(), r.seqno(), r.op()))
            .toList();
        return new CachedDataRowsResponse(rows, result.total(), limit);
    }

    @Post(value = "/ingest", consumes = MediaType.APPLICATION_OCTET_STREAM)
    public HttpResponse<?> ingest(@Body byte[] body) throws Exception {
        List<LsmRow> rows;
        try (Timed t = Timed.start("ingest.decode"); ArrowDecoder decoder = new ArrowDecoder()) {
            rows = decoder.decodeAll(body);
        }
        ingestService.ingestBatch(rows);
        return HttpResponse.accepted();
    }
}
