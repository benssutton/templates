package com.example.template.mcp;

import com.example.template.config.AppSettings;
import com.example.template.service.HealthService;
import jakarta.inject.Singleton;

import java.util.List;
import java.util.Map;

/**
 * Minimal MCP (Model Context Protocol) server implemented over JSON-RPC 2.0.
 *
 * <p>This is the Netty-friendly equivalent of the Python template's FastMCP surface: the
 * official MCP Java SDK ships servlet / SSE transports that do not bridge cleanly onto
 * Micronaut's Netty runtime, so we expose the same logical contract directly as JSON-RPC.
 *
 * <p>Mirrors the Python template's single example tool, {@code get_health_status}, which
 * returns the application's health status string. Resources and prompts are intentionally
 * out of scope (YAGNI) — the Python side leaves them as stubs too.
 *
 * <p>{@link McpController} owns the HTTP/transport concern; this bean owns the protocol
 * logic so it can be unit-tested without an HTTP round trip.
 */
@Singleton
public class McpServer {

    /** Protocol revision advertised in {@code initialize}; matches the MCP spec date string. */
    static final String PROTOCOL_VERSION = "2024-11-05";

    static final String TOOL_NAME = "get_health_status";
    static final String TOOL_DESCRIPTION = "Returns the current application health status.";

    private final AppSettings settings;
    private final HealthService healthService;

    public McpServer(AppSettings settings, HealthService healthService) {
        this.settings = settings;
        this.healthService = healthService;
    }

    /** Dispatch a single JSON-RPC request to the matching MCP method. */
    JsonRpcResponse handle(JsonRpcRequest request) {
        String method = request.method();
        if (method == null) {
            return JsonRpcResponse.fail(request.id(), -32600, "Invalid Request: missing method");
        }
        return switch (method) {
            case "initialize" -> JsonRpcResponse.ok(request.id(), initializeResult());
            case "tools/list" -> JsonRpcResponse.ok(request.id(), toolsListResult());
            case "tools/call" -> toolsCall(request);
            // Notifications (e.g. notifications/initialized) carry no id and need no result.
            case "ping" -> JsonRpcResponse.ok(request.id(), Map.of());
            default -> JsonRpcResponse.fail(request.id(), -32601, "Method not found: " + method);
        };
    }

    private Map<String, Object> initializeResult() {
        return Map.of(
            "protocolVersion", PROTOCOL_VERSION,
            "serverInfo", Map.of(
                "name", settings.getMcpName(),
                "version", settings.getAppVersion()),
            "instructions", settings.getMcpInstructions(),
            "capabilities", Map.of("tools", Map.of()));
    }

    private Map<String, Object> toolsListResult() {
        Map<String, Object> tool = Map.of(
            "name", TOOL_NAME,
            "description", TOOL_DESCRIPTION,
            "inputSchema", Map.of(
                "type", "object",
                "properties", Map.of()));
        return Map.of("tools", List.of(tool));
    }

    private JsonRpcResponse toolsCall(JsonRpcRequest request) {
        Map<String, Object> params = request.params();
        Object name = params == null ? null : params.get("name");
        if (!TOOL_NAME.equals(name)) {
            return JsonRpcResponse.fail(request.id(), -32602, "Unknown tool: " + name);
        }
        return JsonRpcResponse.ok(request.id(), callGetHealthStatus());
    }

    /**
     * Invoke the {@code get_health_status} tool. Exposed package-private so the tool wiring
     * can be asserted directly in a unit test. Returns an MCP {@code tools/call} result:
     * {@code {content:[{type:"text", text:<status>}]}}.
     */
    Map<String, Object> callGetHealthStatus() {
        String status = healthService.detailedStatus().app().status();
        return Map.of("content", List.of(Map.of("type", "text", "text", status)));
    }
}
