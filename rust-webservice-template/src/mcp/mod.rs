//! `/mcp`: rmcp's streamable-HTTP server, mounted as a route so it shares
//! the app's middleware (request id, auth, JSON error bodies, metrics).

pub mod tools;

use std::sync::Arc;

use axum::Router;
use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
};

use crate::state::AppState;
use tools::McpHandler;

/// `Origin` values accepted: each allowed host over http and https. An entry
/// without a port matches any port, as for `Host`.
pub fn allowed_origins(hosts: &[String]) -> Vec<String> {
    hosts
        .iter()
        .flat_map(|h| [format!("http://{h}"), format!("https://{h}")])
        .collect()
}

pub fn router(state: AppState) -> Router<AppState> {
    // DNS-rebinding protection: rmcp checks the Host header against
    // `mcp_allowed_hosts` and a browser's Origin against the same hosts,
    // answering 403 otherwise. Settings refuses an empty host list, which
    // rmcp would treat as "allow every host".
    let hosts = state.settings.mcp_allowed_hosts.clone();
    let config = StreamableHttpServerConfig::default()
        .with_allowed_origins(allowed_origins(&hosts))
        .with_allowed_hosts(hosts)
        // Open SSE streams end when shutdown starts, so they cannot hold
        // the HTTP drain open until its grace period runs out.
        .with_cancellation_token(state.shutdown.token().child_token());
    let service: StreamableHttpService<McpHandler, LocalSessionManager> =
        StreamableHttpService::new(
            move || Ok(McpHandler::new(state.clone())),
            Arc::new(LocalSessionManager::default()),
            config,
        );
    // `route_service` makes it a matched route, so metrics label it
    // `handler="/mcp"`. rmcp handles every method itself (GET/POST/DELETE).
    Router::new().route_service("/mcp", service)
}

#[cfg(test)]
mod tests {
    #[test]
    fn origins_cover_both_schemes_of_every_host() {
        let origins = super::allowed_origins(&["localhost".into(), "app:8443".into()]);
        assert_eq!(
            origins,
            [
                "http://localhost",
                "https://localhost",
                "http://app:8443",
                "https://app:8443"
            ]
        );
    }
}
