package com.example.template.controller;

import com.example.template.core.Timed;
import com.example.template.dto.DataRowsResponse;
import com.example.template.dto.IngestAccepted;
import com.example.template.ingestion.ArrowDecoder;
import com.example.template.ingestion.MalformedBatchException;
import com.example.template.persistence.streamstore.LsmRow;
import com.example.template.service.CachedDataService;
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
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.util.List;

/**
 * GET /data (ClickHouse, ordered by id), GET /data/cache (LSM, merged view by id), POST
 * /data/ingest.
 */
@Controller("/data")
@Tag(name = "data")
@ExecuteOn(TaskExecutors.BLOCKING)
public class DataController {

    private final DataService dataService;
    private final CachedDataService cachedData;
    private final StreamIngestService ingestService;
    private final ArrowDecoder decoder;

    public DataController(
            DataService dataService,
            CachedDataService cachedData,
            StreamIngestService ingestService,
            ArrowDecoder decoder) {
        this.dataService = dataService;
        this.cachedData = cachedData;
        this.ingestService = ingestService;
        this.decoder = decoder;
    }

    @Get
    @Operation(summary = "Analytics rows from ClickHouse, ordered by id")
    public DataRowsResponse getData(@QueryValue(defaultValue = "10") @Min(1) @Max(100) int limit) {
        return dataService.getData(limit);
    }

    @Get("/cache")
    @Operation(summary = "Merged live rows of the in-memory stream store, ordered by id")
    public DataRowsResponse getCachedData(
            @QueryValue(defaultValue = "10") @Min(1) @Max(100) int limit) {
        return cachedData.query(limit);
    }

    /**
     * 202 when stored. Every batch of the stream is decoded and validated before any row is
     * written, so a bad or oversized batch (400/422/413 via {@link MalformedBatchExceptionHandler})
     * stores nothing.
     */
    @Post(
            value = "/ingest",
            consumes = {ARROW_STREAM, MediaType.APPLICATION_OCTET_STREAM})
    @Operation(summary = "Ingest an Arrow IPC stream into the stream store (202)")
    public HttpResponse<IngestAccepted> ingest(@Body byte[] body) {
        List<LsmRow> rows;
        try (Timed t = Timed.start("ingest.decode")) {
            rows = decoder.decodeAll(body);
        } catch (MalformedBatchException e) {
            ingestService.countRejected();
            throw e;
        }
        ingestService.ingestBatch(rows);
        return HttpResponse.accepted().body(IngestAccepted.INSTANCE);
    }

    /** The IANA media type for an Arrow IPC stream. */
    public static final String ARROW_STREAM = "application/vnd.apache.arrow.stream";
}
