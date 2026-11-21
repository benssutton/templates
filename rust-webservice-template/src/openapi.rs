//! The OpenAPI document. Paths come from the handlers' `#[utoipa::path]`
//! through `utoipa_axum::OpenApiRouter` (see `app::router`); this module adds
//! what the routes cannot say: tags, the error schema, `/mcp` (served by
//! rmcp, not a handler), the service identity from settings, and the bearer
//! scheme when authentication is on.

use crate::schemas::error::ErrorResponse;
use crate::settings::Settings;
use utoipa::OpenApi;
use utoipa::openapi::path::{HttpMethod, OperationBuilder};
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityRequirement, SecurityScheme};
use utoipa::openapi::{OpenApi as Document, Response};

#[derive(OpenApi)]
#[openapi(
    components(schemas(ErrorResponse)),
    tags(
        (name = "API Root Page", description = "Service information"),
        (name = "Application Health", description = "Liveness, readiness and detailed status endpoints"),
        (name = "Observability", description = "Prometheus metrics"),
        (name = "Configuration", description = "Key-value configuration in Postgres"),
        (name = "Data Service", description = "ClickHouse rows and the in-memory LSM store"),
        (name = "Cache", description = "Redis cache entries"),
        (name = "MCP", description = "Model Context Protocol (streamable HTTP); tools mirror the REST endpoints"),
    )
)]
pub struct ApiDoc;

pub const BEARER: &str = "bearer";

/// Finish the document built from the routes.
pub fn finish(mut doc: Document, settings: &Settings) -> Document {
    doc.info.title = settings.app_title.clone();
    doc.info.version = settings.app_version.clone();
    doc.info.description = Some(settings.app_description.clone());
    let mcp = |summary: &str| {
        OperationBuilder::new()
            .tag("MCP")
            .summary(Some(summary))
            .response("200", Response::new("JSON-RPC response or SSE stream"))
            .response("403", Response::new("Host or Origin not allowed"))
            .build()
    };
    doc.paths.add_path_operation(
        "/mcp",
        vec![HttpMethod::Post],
        mcp("Send a JSON-RPC message (initialize, tools/list, tools/call)"),
    );
    doc.paths.add_path_operation(
        "/mcp",
        vec![HttpMethod::Get],
        mcp("Open the server event stream"),
    );
    doc.paths
        .add_path_operation("/mcp", vec![HttpMethod::Delete], mcp("End the session"));
    if settings.auth_enabled {
        if let Some(components) = doc.components.as_mut() {
            components.add_security_scheme(
                BEARER,
                SecurityScheme::Http(
                    HttpBuilder::new()
                        .scheme(HttpAuthScheme::Bearer)
                        .bearer_format("JWT")
                        .build(),
                ),
            );
        }
        doc.security = Some(vec![SecurityRequirement::new(BEARER, Vec::<String>::new())]);
    }
    doc
}
