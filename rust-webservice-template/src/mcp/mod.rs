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

    Router::new().fallback_service(service)
}
