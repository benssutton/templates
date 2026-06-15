package com.example.template.mcp;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.annotation.Produces;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;

/**
 * MCP endpoint at {@code /mcp}, mirroring the Python template's MCP mount.
 *
 * <p>Transport is a single JSON-RPC 2.0 POST (streamable-HTTP style): clients POST a
 * JSON-RPC request and receive a JSON-RPC response. This keeps the surface entirely on
 * Micronaut's Netty stack rather than relying on the official SDK's servlet/SSE transports,
 * which do not bridge onto Netty cleanly.
 *
 * <p>Runs on the blocking (virtual-thread) executor, matching every other controller in
 * this template, since {@link McpServer#handle} touches {@code HealthService} probes.
 */
@Controller("/mcp")
@ExecuteOn(TaskExecutors.BLOCKING)
public class McpController {

    private final McpServer server;

    public McpController(McpServer server) {
        this.server = server;
    }

    @Post
    @Produces(MediaType.APPLICATION_JSON)
    public JsonRpcResponse rpc(@Body JsonRpcRequest request) {
        return server.handle(request);
    }

    /**
     * GET is part of the streamable-HTTP transport (used to open a server-to-client SSE
     * stream). This server is request/response only and has no server-initiated messages,
     * so it advertises that the route exists (not 404) while declining the upgrade.
     */
    @Get
    public HttpResponse<?> stream() {
        return HttpResponse.status(io.micronaut.http.HttpStatus.METHOD_NOT_ALLOWED)
            .body("MCP endpoint accepts JSON-RPC over POST.");
    }
}
