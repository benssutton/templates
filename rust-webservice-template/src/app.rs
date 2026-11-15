//! `create_app` (Python main.create_app): a fully isolated app per call:
//! its own state, metrics registry and shutdown handle, so many apps can
//! run in one test process.

use crate::core::{boundary_timing, correlation, cors, last_request, request_limits};
use crate::health::ingest::NoopIngestHealth;
use crate::mcp;
use crate::openapi::ApiDoc;
use crate::routers::{health, metrics, root};
use crate::services;
use crate::settings::Settings;
use crate::state::AppState;
use axum::{
    http::{HeaderName, StatusCode},
    middleware::{from_fn, from_fn_with_state},
    routing::get,
    Json, Router,
};
use std::sync::Arc;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

pub struct App {
    pub router: Router,
    pub state: AppState,
}

impl App {
    /// Final step of the ordered shutdown, called by `server::serve` after
    /// in-flight HTTP has drained. R2 closes the store pools here. R3 closes
    /// the consumer when `Shutdown` fires, before the drain completes.
    pub async fn close(&self) {
        self.state.shutdown.trigger("app closed");
    }
}

/// Build the app. Fails (and `main` exits non-zero) on invalid settings;
/// from R2 on, also when a store cannot be reached within the retry budget.
pub async fn create_app(settings: Settings) -> anyhow::Result<App> {
    let state = AppState::new(settings, Vec::new(), Arc::new(NoopIngestHealth))?;
    Ok(App {
        router: router(state.clone()),
        state,
    })
}

/// FastAPI's 404 body: `{"detail": "Not Found"}`.
async fn not_found() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({ "detail": "Not Found" })),
    )
}

/// FastAPI's 405 body: `{"detail": "Method Not Allowed"}` (axum still adds
/// the `Allow` header).
async fn method_not_allowed() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        Json(serde_json::json!({ "detail": "Method Not Allowed" })),
    )
}

/// All routes + the spec §3 middleware stack. Outermost first:
/// body limit -> correlation ID -> CORS -> Server-Timing -> last-request
/// -> HTTP metrics -> router. `.layer()` wraps outward, so the calls below
/// run innermost first.
///
/// Every layer is attached with `Router::layer`, which wraps each route (and
/// the fallback) individually, i.e. after path matching. That is what lets
/// the metrics middleware read `MatchedPath` (the route template) while the
/// relative order above still holds for every request.
pub fn router(state: AppState) -> Router {
    let settings = state.settings.clone();

    let mut routes: Router<AppState> = Router::new()
        .merge(SwaggerUi::new("/docs").url("/api-doc/openapi.json", ApiDoc::openapi()))
        .route("/", get(root::get_root))
        .route("/health/live", get(health::get_live))
        .route("/health/ready", get(health::get_ready))
        .route("/health/status", get(health::get_status))
        .merge(mcp::build_router(state.clone()));
    if state.metrics.is_some() {
        routes = routes.route("/metrics", get(metrics::get_metrics));
    }
    // Explicit fallbacks so unknown paths and wrong methods get FastAPI's JSON
    // 404/405 bodies and pass through the same middleware stack (correlation
    // ID, Server-Timing, ...).
    let routes = routes
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed);
    let mut app: Router = routes.with_state(state.clone());

    if let Some(m) = state.metrics.clone() {
        app = app.layer(from_fn_with_state(m, services::metrics::middleware));
    }
    let correlation_header = HeaderName::from_bytes(settings.correlation_id_header.as_bytes())
        .unwrap_or_else(|_| HeaderName::from_static("x-request-id"));
    let app = app
        .layer(from_fn_with_state(
            state.last_request.clone(),
            last_request::middleware,
        ))
        .layer(from_fn(boundary_timing::middleware))
        .layer(cors::layer(&settings))
        .layer(from_fn_with_state(
            correlation_header,
            correlation::middleware,
        ));
    request_limits::apply(app, settings.max_request_body_bytes)
}
