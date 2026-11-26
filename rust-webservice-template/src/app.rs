//! `create_app`: a fully isolated app per call (its own state, metrics
//! registry and shutdown handle), so many apps can run in one test process.

use crate::core::{
    auth, boundary_timing, compression, cors, json_errors, last_request, request_id, telemetry,
};
use crate::error::AppError;
use crate::ingestion;
use crate::mcp;
use crate::openapi::{self, ApiDoc};
use crate::persistence::Stores;
use crate::routers::{cache, config, data, health, metrics, root};
use crate::services;
use crate::settings::Settings;
use crate::state::AppState;
use axum::error_handling::HandleErrorLayer;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{HeaderName, Request as HttpRequest, Response, StatusCode};
use axum::middleware::{from_fn, from_fn_with_state, map_request_with_state, map_response};
use axum::{BoxError, Router};
use std::time::Duration;
use tower::ServiceBuilder;
use tower::limit::GlobalConcurrencyLimitLayer;
use tower::util::MapRequest;
use tower_http::decompression::RequestDecompressionLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::map_request_body::MapRequestBodyLayer;
use tower_http::request_id::{PropagateRequestIdLayer, SetRequestIdLayer};
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::{MakeSpan, OnResponse, TraceLayer};
use tracing::Span;
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;
use utoipa_swagger_ui::SwaggerUi;

/// The app's router behind the trailing-slash rewrite.
pub type Normalised<B> = MapRequest<Router, fn(HttpRequest<B>) -> HttpRequest<B>>;

pub struct App {
    pub router: Router,
    pub state: AppState,
}

impl App {
    /// The router behind trailing-slash normalisation (`/config/` is
    /// `/config`). It must wrap the router from outside: a layer added with
    /// `Router::layer` runs after routing, too late to change the path.
    pub fn service<B>(&self) -> Normalised<B> {
        MapRequest::new(
            self.router.clone(),
            trim_trailing_slash::<B> as fn(HttpRequest<B>) -> HttpRequest<B>,
        )
    }

    /// Final step of the ordered shutdown (consumer, HTTP drain, pools). The
    /// consumer loop stopped when `Shutdown` fired and `server::serve` has
    /// drained HTTP, so close the consumer, join the ingest task, then close
    /// the stores.
    pub async fn close(&self) {
        self.state.shutdown.trigger("app closed");
        self.state.ingest.close().await;
        self.state.stores.close().await;
    }
}

/// Build the app. Fails (and `main` exits non-zero) on invalid settings, or
/// when a store or the ingest transport cannot be reached within the retry
/// budget (`connect_max_attempts`).
pub async fn create_app(settings: Settings) -> anyhow::Result<App> {
    // Settings built in code have not been through `from_env`; an invalid
    // config fails here, before any retry budget is spent.
    settings.check()?;
    let stores = Stores::connect(&settings).await?;
    // From here on, a failure must close what is already connected (the
    // Postgres pool and the broker link would otherwise leak).
    let consumer = match ingestion::connect(&settings).await {
        Ok(consumer) => consumer,
        Err(err) => {
            stores.close().await;
            return Err(err);
        }
    };
    let probes = stores.probes();
    let state = match AppState::new(settings, stores.clone(), probes, consumer.clone()) {
        Ok(state) => state,
        Err(err) => {
            consumer.close().await;
            stores.close().await;
            return Err(err);
        }
    };
    Ok(App {
        router: router(state.clone()),
        state,
    })
}

/// `/a/b/` -> `/a/b`, query kept. `/docs/` is left alone: Swagger UI
/// redirects `/docs` to `/docs/`, so trimming it would loop. tower-http's
/// `NormalizePath` has no exception list, hence this small rewrite.
fn trim_trailing_slash<B>(mut req: HttpRequest<B>) -> HttpRequest<B> {
    let path = req.uri().path();
    if path.len() <= 1 || !path.ends_with('/') || path.starts_with("/docs") {
        return req;
    }
    let trimmed = path.trim_end_matches('/');
    let trimmed = if trimmed.is_empty() { "/" } else { trimmed };
    let path_and_query = match req.uri().query() {
        Some(query) => format!("{trimmed}?{query}"),
        None => trimmed.to_owned(),
    };
    let mut parts = req.uri().clone().into_parts();
    if let Ok(pq) = path_and_query.parse() {
        parts.path_and_query = Some(pq);
        if let Ok(uri) = axum::http::Uri::from_parts(parts) {
            *req.uri_mut() = uri;
        }
    }
    req
}

async fn overloaded(err: BoxError) -> AppError {
    AppError::Unavailable(format!("request shed: {err}"))
}

async fn strip_unacceptable_id(State(header): State<HeaderName>, req: Request) -> Request {
    request_id::drop_unacceptable(&header, req)
}

/// The request span: every log line inside a request carries its id.
#[derive(Clone, Copy)]
struct RequestSpan;

impl<B> MakeSpan<B> for RequestSpan {
    fn make_span(&mut self, req: &axum::http::Request<B>) -> Span {
        let span = tracing::info_span!(
            "request",
            request_id = %request_id::of(req),
            method = %req.method(),
            path = %req.uri().path(),
        );
        telemetry::set_remote_parent(&span, req.headers());
        span
    }
}

/// One access-log line per request, inside the request span.
#[derive(Clone, Copy)]
struct AccessLog;

impl<B> OnResponse<B> for AccessLog {
    fn on_response(self, response: &Response<B>, latency: Duration, _: &Span) {
        tracing::info!(
            status = response.status().as_u16(),
            latency_ms = latency.as_secs_f64() * 1000.0,
            "request completed"
        );
    }
}

/// All routes and the middleware stack. Outermost first: request id ->
/// access log -> content-coding negotiation (406, 415, Vary) -> response
/// compression -> JSON error bodies -> request decompression -> load
/// shedding -> timeout -> CORS -> body limit -> Server-Timing ->
/// last-request -> auth -> HTTP metrics -> handler. Decompression sits
/// outside the body limit, so the limit counts decompressed bytes.
///
/// Every layer is attached with `Router::layer`, which wraps each route (and
/// the fallback) after path matching, so the metrics layer can read
/// `MatchedPath`. The concurrency limit is a global one for that reason: a
/// plain `ConcurrencyLimitLayer` would give each route its own budget.
pub fn router(state: AppState) -> Router {
    let settings = state.settings.clone();
    let shed = |limit: usize| {
        ServiceBuilder::new()
            .layer(HandleErrorLayer::new(overloaded))
            .load_shed()
            .layer(GlobalConcurrencyLimitLayer::new(limit))
    };

    // Decoded Arrow bodies wait for the single LSM writer; bounding the
    // requests in flight bounds that memory.
    let ingest = OpenApiRouter::new()
        .routes(routes!(data::ingest_batch))
        .route_layer(shed(settings.ingest_max_in_flight));
    let mut api = OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(root::get_root))
        .routes(routes!(health::get_live))
        .routes(routes!(health::get_ready))
        .routes(routes!(health::get_status))
        .routes(routes!(config::get_config, config::set_config))
        .routes(routes!(data::get_data))
        .routes(routes!(data::get_cached_data))
        .routes(routes!(cache::set_cache))
        .routes(routes!(cache::get_cache))
        .merge(ingest);
    if state.metrics.is_some() {
        api = api.routes(routes!(metrics::get_metrics));
    }
    let (routes, doc) = api.split_for_parts();
    let doc = openapi::finish(doc, &settings);
    let mut app: Router = routes
        .merge(SwaggerUi::new("/docs").url("/api-doc/openapi.json", doc))
        .merge(mcp::router(state.clone()))
        .with_state(state.clone());

    if let Some(m) = state.metrics.clone() {
        app = app.layer(from_fn_with_state(m, services::metrics::middleware));
    }
    let request_id_header = settings.correlation_id_header.clone();
    if let Some(authenticator) = state.auth.clone() {
        app = app.layer(from_fn_with_state(authenticator, auth::middleware));
    }
    // `Router::layer` wraps outward: the calls below run innermost first.
    app.layer(from_fn_with_state(
        state.last_request.clone(),
        last_request::middleware,
    ))
    .layer(from_fn(boundary_timing::middleware))
    .layer(RequestBodyLimitLayer::new(settings.max_request_body_bytes))
    .layer(DefaultBodyLimit::disable())
    .layer(cors::layer(&settings))
    .layer(TimeoutLayer::with_status_code(
        StatusCode::SERVICE_UNAVAILABLE,
        crate::core::retry::seconds(settings.request_timeout_seconds),
    ))
    .layer(shed(settings.max_concurrent_requests))
    .layer(
        ServiceBuilder::new()
            .layer(RequestDecompressionLayer::new())
            .layer(MapRequestBodyLayer::new(axum::body::Body::new)),
    )
    .layer(map_response(json_errors::ensure_json))
    .layer(compression::response_layer(&settings))
    .layer(from_fn_with_state(
        compression::Negotiation::new(&settings),
        compression::negotiate,
    ))
    .layer(
        TraceLayer::new_for_http()
            .make_span_with(RequestSpan)
            .on_request(())
            .on_response(AccessLog)
            .on_failure(()),
    )
    .layer(PropagateRequestIdLayer::new(request_id_header.clone()))
    .layer(SetRequestIdLayer::new(
        request_id_header.clone(),
        request_id::NewRequestId,
    ))
    .layer(map_request_with_state(
        request_id_header,
        strip_unacceptable_id,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trimmed(uri: &str) -> String {
        let req = Request::builder()
            .uri(uri)
            .body(axum::body::Body::empty())
            .unwrap();
        trim_trailing_slash(req).uri().to_string()
    }

    #[test]
    fn trailing_slashes_are_trimmed_except_root_and_docs() {
        assert_eq!(trimmed("/config/"), "/config");
        assert_eq!(trimmed("/cache//"), "/cache");
        assert_eq!(trimmed("/data/?limit=2"), "/data?limit=2");
        assert_eq!(trimmed("/"), "/");
        assert_eq!(trimmed("/docs/"), "/docs/");
        assert_eq!(trimmed("/config"), "/config");
    }
}
