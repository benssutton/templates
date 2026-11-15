//! Maximum inbound request body (Python core/request_limits.py), app-wide.
//! tower-http's RequestBodyLimitLayer answers 413 up front for an oversized
//! Content-Length, and otherwise counts streamed bytes: once a chunked body
//! crosses the limit, axum's body extractors fail with LengthLimitError,
//! which axum maps to 413. axum's own 2 MiB DefaultBodyLimit is disabled so
//! `max_request_body_bytes` is the single limit.

use axum::{
    extract::DefaultBodyLimit,
    http::{header, HeaderValue, StatusCode},
    middleware::map_response,
    response::{IntoResponse, Response},
    Router,
};
use tower_http::limit::RequestBodyLimitLayer;

/// Python's `_reject` response: 413, `text/plain; charset=utf-8`, `Payload Too Large`.
fn python_413() -> Response {
    (
        StatusCode::PAYLOAD_TOO_LARGE,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        )],
        "Payload Too Large",
    )
        .into_response()
}

/// Rewrites EVERY 413 (the limit layer's, the extractor's, and a handler's own
/// `AppError::PayloadTooLarge`) into Python's exact response.
async fn normalise_413(response: Response) -> Response {
    if response.status() == StatusCode::PAYLOAD_TOO_LARGE {
        python_413()
    } else {
        response
    }
}

pub fn apply<S>(router: Router<S>, max_bytes: usize) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router
        .layer(DefaultBodyLimit::disable())
        .layer(RequestBodyLimitLayer::new(max_bytes))
        .layer(map_response(normalise_413))
}
