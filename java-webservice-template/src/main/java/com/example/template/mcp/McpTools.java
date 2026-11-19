package com.example.template.mcp;

import com.example.template.dto.CacheEntry;
import com.example.template.dto.CachedDataRowsResponse;
import com.example.template.dto.ConfigEntry;
import com.example.template.dto.DataRowsResponse;
import com.example.template.dto.ErrorDetail;
import com.example.template.dto.health.ReadinessResponse;
import com.example.template.service.CacheService;
import com.example.template.service.CachedDataService;
import com.example.template.service.ConfigService;
import com.example.template.service.DataService;
import com.example.template.service.HealthService;
import io.micronaut.json.JsonMapper;
import io.micronaut.mcp.annotations.Tool;
import io.micronaut.mcp.annotations.ToolArg;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.modelcontextprotocol.spec.McpSchema;
import jakarta.inject.Singleton;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import java.io.IOException;
import java.util.List;

/**
 * MCP tools at {@code /mcp}, served by micronaut-mcp (the official MCP Java SDK's stateless
 * Streamable HTTP server). Each tool mirrors a REST read endpoint and calls the same service, so
 * REST and MCP never disagree. Results are serialised with the same snake_case JSON as the REST
 * bodies.
 */
@Singleton
@ExecuteOn(TaskExecutors.BLOCKING)
public class McpTools {

    private final HealthService health;
    private final ConfigService config;
    private final CacheService cache;
    private final DataService data;
    private final CachedDataService cachedData;
    private final JsonMapper json;

    public McpTools(
            HealthService health,
            ConfigService config,
            CacheService cache,
            DataService data,
            CachedDataService cachedData,
            JsonMapper json) {
        this.json = json;
        this.health = health;
        this.config = config;
        this.cache = cache;
        this.data = data;
        this.cachedData = cachedData;
    }

    /** GET /health/ready. */
    @Tool(
            name = "get_health_status",
            description = "Readiness of the service and each dependency (GET /health/ready).")
    public ReadinessResponse getHealthStatus() {
        return health.readiness();
    }

    /** GET /config. */
    @Tool(
            name = "get_config",
            description = "All configuration entries, ordered by key (GET /config).")
    public List<ConfigEntry> getConfig() {
        return config.getAll();
    }

    /**
     * GET /cache/{key}. A missing key is a tool result with {@code isError} (the endpoint's 404); a
     * thrown exception would surface as a JSON-RPC protocol error instead.
     */
    @Tool(
            name = "get_cache",
            description = "One cached JSON value and its remaining TTL (GET /cache/{key}).")
    public McpSchema.CallToolResult getCache(
            @ToolArg(name = "key", description = "Cache key") @NotBlank String key)
            throws IOException {
        CacheEntry entry = cache.get(key);
        if (entry == null) {
            return McpSchema.CallToolResult.builder()
                    .isError(true)
                    .addTextContent(
                            json.writeValueAsString(new ErrorDetail("Key '" + key + "' not found")))
                    .build();
        }
        return McpSchema.CallToolResult.builder()
                .addTextContent(json.writeValueAsString(entry))
                .build();
    }

    /** GET /data. */
    @Tool(
            name = "get_data",
            description = "Rows from the analytics store, ordered by id (GET /data).")
    public DataRowsResponse getData(
            @ToolArg(name = "limit", description = "Rows to return, 1-100") @Min(1) @Max(100)
                    int limit) {
        return data.getData(limit);
    }

    /** GET /data/cache. */
    @Tool(
            name = "get_cached_data",
            description = "Newest rows of the in-memory stream store (GET /data/cache).")
    public CachedDataRowsResponse getCachedData(
            @ToolArg(name = "limit", description = "Rows to return, 1-100") @Min(1) @Max(100)
                    int limit) {
        return cachedData.query(limit);
    }
}
