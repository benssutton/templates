//! CORS from settings (Python: Starlette CORSMiddleware). Settings::validate
//! already rejects credentials + wildcard origin. For credentials +
//! wildcard methods/headers, tower-http would panic, so the request's own
//! method/headers are mirrored (what Starlette does in that case).

use crate::settings::Settings;
use axum::http::{HeaderName, HeaderValue, Method};
use std::time::Duration;
use tower_http::cors::{AllowHeaders, AllowMethods, AllowOrigin, CorsLayer};

fn wildcard(values: &[String]) -> bool {
    values.iter().any(|v| v == "*")
}

pub fn layer(s: &Settings) -> CorsLayer {
    let origin = if wildcard(&s.cors_allow_origins) {
        AllowOrigin::any()
    } else {
        AllowOrigin::list(
            s.cors_allow_origins
                .iter()
                .filter_map(|o| HeaderValue::from_str(o).ok()),
        )
    };
    let methods = match (wildcard(&s.cors_allow_methods), s.cors_allow_credentials) {
        (true, false) => AllowMethods::any(),
        (true, true) => AllowMethods::mirror_request(),
        (false, _) => AllowMethods::list(
            s.cors_allow_methods
                .iter()
                .filter_map(|m| Method::from_bytes(m.as_bytes()).ok()),
        ),
    };
    let headers = match (wildcard(&s.cors_allow_headers), s.cors_allow_credentials) {
        (true, false) => AllowHeaders::any(),
        (true, true) => AllowHeaders::mirror_request(),
        (false, _) => AllowHeaders::list(
            s.cors_allow_headers
                .iter()
                .filter_map(|h| HeaderName::from_bytes(h.as_bytes()).ok()),
        ),
    };
    CorsLayer::new()
        .allow_origin(origin)
        .allow_methods(methods)
        .allow_headers(headers)
        .allow_credentials(s.cors_allow_credentials)
        // Starlette CORSMiddleware default max_age=600.
        .max_age(Duration::from_secs(600))
}

#[cfg(test)]
mod tests {
    use crate::settings::Settings;
    use axum::{routing::get, Router};

    /// tower-http panics at layer time on credentials + wildcard methods or
    /// headers; we mirror the request instead. Building must not panic.
    #[test]
    fn credentials_with_wildcard_methods_and_headers_builds() {
        let s = Settings {
            cors_allow_credentials: true,
            cors_allow_origins: vec!["https://app.example.com".into()],
            ..Settings::default()
        };
        let _router: Router = Router::new()
            .route("/", get(|| async { "ok" }))
            .layer(super::layer(&s));
    }

    #[test]
    fn explicit_lists_build() {
        let s = Settings {
            cors_allow_origins: vec!["https://a.example.com".into()],
            cors_allow_methods: vec!["GET".into(), "POST".into()],
            cors_allow_headers: vec!["content-type".into(), "x-request-id".into()],
            ..Settings::default()
        };
        let _router: Router = Router::new()
            .route("/", get(|| async { "ok" }))
            .layer(super::layer(&s));
    }
}
