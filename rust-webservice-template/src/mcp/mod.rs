pub mod tools;

use std::sync::Arc;

use axum::Router;
use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
};

use crate::state::AppState;
use tools::McpHandler;

pub fn build_router(state: AppState) -> Router<AppState> {
    let service: StreamableHttpService<McpHandler, LocalSessionManager> =
        StreamableHttpService::new(
            move || Ok(McpHandler::new(state.clone())),
            Arc::new(LocalSessionManager::default()),
            // SECURITY: disable_allowed_hosts() bypasses DNS-rebinding protection.
            // In production, replace with:
            //   StreamableHttpServerConfig::default()
            //       .with_allowed_hosts(vec![std::env::var("HOST").unwrap_or("localhost".into())])
            StreamableHttpServerConfig::default().disable_allowed_hosts(),
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
