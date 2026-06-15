package com.example.template.mcp;

import io.micronaut.core.annotation.Nullable;
import io.micronaut.serde.annotation.Serdeable;

import java.util.Map;

/**
 * Inbound JSON-RPC 2.0 request envelope for the MCP-over-HTTP endpoint.
 *
 * <p>{@code id} is {@link Object} because JSON-RPC permits a string, number, or null id
 * (notifications carry no id); it is echoed back verbatim in the response.
 */
@Serdeable
public record JsonRpcRequest(
    @Nullable String jsonrpc,
    @Nullable Object id,
    String method,
    @Nullable Map<String, Object> params) {}
