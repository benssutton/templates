//! Correlation ID (Python core/correlation.py). Adopt the inbound header
//! (`correlation_id_header`, default `X-Request-ID`) or generate a 32-hex
//! id; record it on the request's tracing span (so every log line inside
//! the request carries it); echo it on the response. The R3 ingest task
//! opens its own span per delivery with `new_id()`.

use axum::{
    extract::{Request, State},
    http::{HeaderName, HeaderValue},
    middleware::Next,
    response::Response,
};
use tracing::Instrument;

/// The request's correlation ID, available to handlers as an extension.
#[derive(Debug, Clone)]
pub struct CorrelationId(pub String);

/// `uuid4().hex` equivalent: 32 lowercase hex chars.
pub fn new_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

pub async fn middleware(
    State(header): State<HeaderName>,
    mut req: Request,
    next: Next,
) -> Response {
    let id = req
        .headers()
        .get(&header)
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(new_id);
    req.extensions_mut().insert(CorrelationId(id.clone()));
    let span = tracing::info_span!(
        "request",
        correlation_id = %id,
        method = %req.method(),
        path = %req.uri().path()
    );
    let mut response = next.run(req).instrument(span).await;
    if let Ok(value) = HeaderValue::from_str(&id) {
        response.headers_mut().insert(header, value);
    }
    response
}

#[cfg(test)]
mod tests {
    #[test]
    fn new_id_is_32_lowercase_hex_like_python_uuid4_hex() {
        let id = super::new_id();
        assert_eq!(id.len(), 32);
        assert!(id
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
        assert_ne!(id, super::new_id());
    }
}
