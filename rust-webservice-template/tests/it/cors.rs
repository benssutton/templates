use axum::{routing::get, Router};
use rust_template::{core::cors, settings::Settings};

/// A preflight carries Starlette's default `Access-Control-Max-Age: 600`.
/// (Task 13 covers the rest of CORS through the real app.)
#[tokio::test]
async fn preflight_includes_max_age_600() {
    let router: Router = Router::new()
        .route("/", get(|| async { "ok" }))
        .layer(cors::layer(&Settings::default()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

    let r = crate::support::client()
        .request(reqwest::Method::OPTIONS, format!("http://{addr}/"))
        .header("Origin", "https://a.example.com")
        .header("Access-Control-Request-Method", "GET")
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{}", r.status());
    assert_eq!(r.headers()["access-control-max-age"], "600");
}

use crate::support::{test_settings, TestApp};

async fn preflight(app: &TestApp, origin: &str) -> reqwest::Response {
    app.client()
        .request(reqwest::Method::OPTIONS, app.url("/health/live"))
        .header("Origin", origin)
        .header("Access-Control-Request-Method", "GET")
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn preflight_allows_a_configured_origin() {
    let app = TestApp::spawn(Settings {
        cors_allow_origins: vec!["https://app.example.com".into()],
        ..test_settings()
    })
    .await;
    let r = preflight(&app, "https://app.example.com").await;
    assert_eq!(r.status(), 200);
    assert_eq!(
        r.headers()["access-control-allow-origin"],
        "https://app.example.com"
    );
    assert_eq!(r.headers()["access-control-max-age"], "600");

    let other = preflight(&app, "https://evil.example.com").await;
    assert!(other.headers().get("access-control-allow-origin").is_none());
}

#[tokio::test]
async fn wildcard_default_allows_any_origin() {
    let app = TestApp::spawn(test_settings()).await;
    let r = preflight(&app, "https://arbitrary.example.com").await;
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["access-control-allow-origin"], "*");
}

#[tokio::test]
async fn credentials_with_explicit_origin_mirror_methods() {
    let app = TestApp::spawn(Settings {
        cors_allow_origins: vec!["https://app.example.com".into()],
        cors_allow_credentials: true,
        ..test_settings()
    })
    .await;
    let r = preflight(&app, "https://app.example.com").await;
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["access-control-allow-credentials"], "true");
    assert_eq!(r.headers()["access-control-allow-methods"], "GET");
}
