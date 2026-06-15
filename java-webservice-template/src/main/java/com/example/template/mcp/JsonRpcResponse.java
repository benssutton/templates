package com.example.template.mcp;

import io.micronaut.core.annotation.Nullable;
import io.micronaut.serde.annotation.Serdeable;

/**
 * Outbound JSON-RPC 2.0 response envelope. Exactly one of {@code result} / {@code error}
 * is populated. {@code id} echoes the request id (null for parse errors).
 */
@Serdeable
public record JsonRpcResponse(
    String jsonrpc,
    @Nullable Object id,
    @Nullable Object result,
    @Nullable JsonRpcError error) {

    static JsonRpcResponse ok(Object id, Object result) {
        return new JsonRpcResponse("2.0", id, result, null);
    }

    static JsonRpcResponse fail(Object id, int code, String message) {
        return new JsonRpcResponse("2.0", id, null, new JsonRpcError(code, message));
    }

    @Serdeable
    public record JsonRpcError(int code, String message) {}
}
