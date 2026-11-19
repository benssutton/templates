//! Every error response is `application/json` `{"detail": ...}`. Handlers
//! already answer that way through `AppError`; this layer covers the error
//! responses produced elsewhere: tower-http's body limit (plain text),
//! timeout (empty body), and rmcp's Host/Origin and protocol rejections
//! (plain text, or no content type). A short text body becomes the detail;
//! an empty or non-text one is replaced by the status's reason phrase.

use axum::body::{to_bytes, Body};
use axum::http::{header, HeaderValue};
use axum::response::Response;
use serde_json::json;

/// Plain-text bodies up to this size are kept as the detail.
const MAX_TEXT_DETAIL: usize = 512;

fn is_json(response: &Response) -> bool {
    response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("application/json"))
}

/// Plain text, or no content type at all (rmcp's 403 sends none).
fn is_plain_text(response: &Response) -> bool {
    match response.headers().get(header::CONTENT_TYPE) {
        None => true,
        Some(ct) => ct.to_str().is_ok_and(|ct| ct.starts_with("text/plain")),
    }
}

pub async fn ensure_json(response: Response) -> Response {
    let status = response.status();
    if !(status.is_client_error() || status.is_server_error()) || is_json(&response) {
        return response;
    }
    let reason = status.canonical_reason().unwrap_or("Error").to_owned();
    let text = is_plain_text(&response);
    let (mut parts, body) = response.into_parts();
    let detail = if text {
        match to_bytes(body, MAX_TEXT_DETAIL).await {
            Ok(bytes) if !bytes.is_empty() => String::from_utf8(bytes.to_vec())
                .map(|s| s.trim().to_owned())
                .unwrap_or(reason),
            _ => reason,
        }
    } else {
        reason
    };
    let bytes = json!({ "detail": detail }).to_string();
    parts.headers.remove(header::CONTENT_LENGTH);
    parts.headers.remove(header::CONTENT_ENCODING);
    parts.headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    Response::from_parts(parts, Body::from(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    async fn body_of(response: Response) -> (StatusCode, String, serde_json::Value) {
        let status = response.status();
        let ct = response.headers()[header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .to_owned();
        let bytes = to_bytes(response.into_body(), 4096).await.unwrap();
        (status, ct, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn plain_text_errors_keep_their_text_as_detail() {
        let response = (
            StatusCode::FORBIDDEN,
            "Forbidden: Host header is not allowed",
        )
            .into_response();
        let (status, ct, body) = body_of(ensure_json(response).await).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(ct, "application/json");
        assert_eq!(
            body,
            json!({"detail": "Forbidden: Host header is not allowed"})
        );
    }

    #[tokio::test]
    async fn an_untyped_text_body_is_kept_too() {
        let response = axum::http::Response::builder()
            .status(StatusCode::FORBIDDEN)
            .body(Body::from("Forbidden: Origin header is not allowed"))
            .unwrap();
        let (_, _, body) = body_of(ensure_json(response).await).await;
        assert_eq!(
            body,
            json!({"detail": "Forbidden: Origin header is not allowed"})
        );
    }

    #[tokio::test]
    async fn empty_errors_get_the_reason_phrase() {
        let response = StatusCode::SERVICE_UNAVAILABLE.into_response();
        let (_, _, body) = body_of(ensure_json(response).await).await;
        assert_eq!(body, json!({"detail": "Service Unavailable"}));
    }

    #[tokio::test]
    async fn success_and_json_errors_pass_through() {
        let ok = (StatusCode::OK, "fine").into_response();
        assert_eq!(
            ensure_json(ok).await.headers()[header::CONTENT_TYPE],
            "text/plain; charset=utf-8"
        );
        let json_err = (StatusCode::NOT_FOUND, axum::Json(json!({"detail": "x"}))).into_response();
        let (_, _, body) = body_of(ensure_json(json_err).await).await;
        assert_eq!(body, json!({"detail": "x"}));
    }
}
