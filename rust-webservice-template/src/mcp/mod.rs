pub mod tools;

use std::sync::Arc;

use axum::Router;
use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
};

use crate::state::AppState;
use tools::McpHandler;

pub fn build_router(state: AppState) -> Router<AppState> {
    // SECURITY: DNS-rebinding protection. rmcp checks the Host header of every
    // /mcp request against `mcp_allowed_hosts` and answers 403 otherwise.
    // Default ["localhost"]; deployments list their public host names
    // (compose adds 127.0.0.1 and app). Settings::validate() refuses an empty
    // list, which rmcp would treat as "allow every host".
    let config = StreamableHttpServerConfig::default()
        .with_allowed_hosts(state.settings.mcp_allowed_hosts.clone());
    let service: StreamableHttpService<McpHandler, LocalSessionManager> =
        StreamableHttpService::new(
            move || Ok(McpHandler::new(state.clone())),
            Arc::new(LocalSessionManager::default()),
            config,
        );

    // Served at both `/mcp` and `/mcp/` (Python's canonical endpoint:
    // FastMCP `streamable_http_path="/"` mounted at `/mcp`). Both routes share
    // this one service (cloned), hence one session manager, so a session made
    // on either path works on the other. `route_service` (not
    // `fallback_service`) makes them matched routes, so the metrics middleware
    // labels them `handler="/mcp"` and `handler="/mcp/"` instead of "none".
    // axum's `nest` cannot express the trailing-slash path, so the paths are
    // absolute and `app::router` merges this router. rmcp still sees every
    // method (GET/POST/DELETE, its own 405 otherwise).
    Router::new()
        .route_service("/mcp", service.clone())
        .route_service("/mcp/", service)
}
