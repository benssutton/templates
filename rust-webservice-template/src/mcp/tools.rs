//! MCP tools. Each mirrors a REST read endpoint and calls the same service,
//! so a tool and its endpoint can never disagree.

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock, Implementation, ServerCapabilities, ServerInfo};
use rmcp::{ErrorData, ServerHandler, tool, tool_handler, tool_router};
use serde::{Deserialize, Serialize};

use crate::error::AppError;
use crate::schemas::data::{DEFAULT_LIMIT, MAX_LIMIT};
use crate::state::AppState;

#[derive(Clone)]
pub struct McpHandler {
    state: AppState,
    tool_router: ToolRouter<Self>,
}

impl McpHandler {
    pub fn new(state: AppState) -> Self {
        Self {
            state,
            tool_router: Self::tool_router(),
        }
    }
}

#[derive(Debug, Deserialize, rmcp::schemars::JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct LimitArgs {
    /// Rows to return: 1 to 100, default 10.
    pub limit: Option<u32>,
}

impl LimitArgs {
    fn get(&self) -> Result<u32, ErrorData> {
        let limit = self.limit.unwrap_or(DEFAULT_LIMIT);
        if (1..=MAX_LIMIT).contains(&limit) {
            Ok(limit)
        } else {
            Err(ErrorData::invalid_params(
                format!("limit must be between 1 and {MAX_LIMIT}"),
                None,
            ))
        }
    }
}

#[derive(Debug, Deserialize, rmcp::schemars::JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct KeyArgs {
    /// Cache key.
    pub key: String,
}

/// One text item holding the REST body, as every template's tools return.
fn rest_body<T: Serialize>(value: &T) -> Result<CallToolResult, ErrorData> {
    ContentBlock::json(value).map(|c| CallToolResult::success(vec![c]))
}

/// The same detail an HTTP client would get; a 503 or 500 never carries
/// the cause, which is logged instead.
fn tool_error(err: AppError) -> ErrorData {
    use axum::response::IntoResponse;
    let status = err.status();
    let detail = match &err {
        AppError::NotFound(_) | AppError::Unprocessable(_) | AppError::BadRequest(_) => {
            err.to_string()
        }
        _ => status.canonical_reason().unwrap_or("Error").to_owned(),
    };
    // Rendering logs the cause of a 5xx inside the current span.
    drop(err.into_response());
    if status.is_client_error() {
        ErrorData::invalid_params(detail, None)
    } else {
        ErrorData::internal_error(detail, None)
    }
}

/// A missing resource is a tool result with `isError`, carrying the REST 404 body.
fn not_found(err: AppError) -> Result<CallToolResult, ErrorData> {
    ContentBlock::json(serde_json::json!({ "detail": err.to_string() }))
        .map(|c| CallToolResult::error(vec![c]))
}

#[tool_router(router = tool_router)]
impl McpHandler {
    #[tool(
        description = "Detailed health: app, uptime, dependencies, ingest and system (GET /health/status)"
    )]
    async fn get_health_status(&self) -> Result<CallToolResult, ErrorData> {
        rest_body(&self.state.health.detailed_status().await)
    }

    #[tool(description = "Every configuration entry, ordered by key (GET /config)")]
    async fn get_config(&self) -> Result<CallToolResult, ErrorData> {
        let entries = self.state.config.get_all().await.map_err(tool_error)?;
        rest_body(&entries)
    }

    #[tool(description = "A cache entry and its remaining TTL (GET /cache/{key})")]
    async fn get_cache(
        &self,
        Parameters(args): Parameters<KeyArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        match self.state.cache.get(&args.key).await {
            Ok(Some(entry)) => rest_body(&entry),
            Ok(None) => not_found(AppError::NotFound(format!("Key '{}'", args.key))),
            Err(err @ AppError::NotFound(_)) => not_found(err),
            Err(err) => Err(tool_error(err)),
        }
    }

    #[tool(description = "A page of ClickHouse rows ordered by id (GET /data)")]
    async fn get_data(
        &self,
        Parameters(args): Parameters<LimitArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let rows = self
            .state
            .data
            .get_data(args.get()?)
            .await
            .map_err(tool_error)?;
        rest_body(&rows)
    }

    #[tool(
        description = "Live rows of the in-memory stream store ordered by key (GET /data/cache)"
    )]
    async fn get_cached_data(
        &self,
        Parameters(args): Parameters<LimitArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let rows = self
            .state
            .ingest
            .get_data(args.get()?)
            .await
            .map_err(tool_error)?;
        rest_body(&rows)
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for McpHandler {
    /// The rmcp types are `#[non_exhaustive]`, so they are built with constructors.
    fn get_info(&self) -> ServerInfo {
        let settings = &self.state.settings;
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(
                settings.mcp_name.clone(),
                settings.app_version.clone(),
            ))
            .with_instructions(settings.mcp_instructions.clone())
    }
}
