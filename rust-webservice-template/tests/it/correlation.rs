use crate::probes::probe;
use crate::support::{logs, test_settings, TestApp};
use axum::{http::HeaderName, middleware::from_fn_with_state, routing::get, Router};
use rust_template::app::{router, App};
use rust_template::core::correlation;
use rust_template::health::ingest::NoopIngestHealth;
use rust_template::settings::Settings;
use rust_template::state::AppState;
use std::sync::Arc;

#[tokio::test]
async fn echoes_an_inbound_id() {
    let app = TestApp::spawn(test_settings()).await;
    let r = app
        .client()
        .get(app.url("/"))
        .header("X-Request-ID", "trace-42")
        .send()
        .await
        .unwrap();
    assert_eq!(r.headers()["x-request-id"], "trace-42");
}

#[tokio::test]
async fn generates_a_32_hex_id_when_absent() {
    let app = TestApp::spawn(test_settings()).await;
    let r = app.get("/").await;
    let id = r.headers()["x-request-id"].to_str().unwrap().to_owned();
    assert_eq!(id.len(), 32, "{id}");
    assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    let other = app.get("/").await.headers()["x-request-id"]
        .to_str()
        .unwrap()
        .to_owned();
    assert_ne!(id, other, "each request gets a fresh id");
}

#[tokio::test]
async fn header_name_is_configurable() {
    let app = TestApp::spawn(Settings {
        correlation_id_header: "X-Correlation-ID".into(),
        ..test_settings()
    })
    .await;
    let r = app
        .client()
        .get(app.url("/health/live"))
        .header("X-Correlation-ID", "abc")
        .send()
        .await
        .unwrap();
    assert_eq!(r.headers()["x-correlation-id"], "abc");
    assert!(r.headers().get("x-request-id").is_none());
}

#[tokio::test]
async fn error_responses_still_carry_the_id() {
    let app = TestApp::spawn(test_settings()).await;
    let r = app
        .client()
        .get(app.url("/no-such-route"))
        .header("X-Request-ID", "trace-404")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 404);
    assert_eq!(r.headers()["x-request-id"], "trace-404");
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["detail"], "Not Found");
}

/// A log line emitted inside a request carries the `correlation_id` span
/// field. The route is a tiny logging handler behind the real
/// `correlation::middleware` (the real app has no handler that logs on the
/// request task: probes run on their own spawned tasks, outside the span).
#[tokio::test]
async fn log_lines_inside_a_request_carry_the_correlation_id() {
    logs::ensure_installed();

    let router = Router::new()
        .route(
            "/log",
            get(|| async {
                tracing::info!("handled inside the request");
                "ok"
            }),
        )
        .layer(from_fn_with_state(
            HeaderName::from_bytes(test_settings().correlation_id_header.as_bytes()).unwrap(),
            correlation::middleware,
        ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

    let r = crate::support::client()
        .get(format!("http://{addr}/log"))
        .header("X-Request-ID", "trace-log-1")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    let lines = logs::lines_containing("trace-log-1");
    assert!(
        lines
            .iter()
            .any(|l| l.contains("handled inside the request")
                && l.contains("correlation_id=trace-log-1")),
        "no log line carrying the correlation id: {lines:#?}"
    );
}

/// Through the real app: a failing probe's log line (emitted on a spawned
/// task) still carries the request's correlation id.
#[tokio::test]
async fn probe_failure_logs_carry_the_correlation_id() {
    logs::ensure_installed();
    let state = AppState::new(
        test_settings(),
        vec![probe("redis", Err("refused"), 0)],
        Arc::new(NoopIngestHealth),
    )
    .unwrap();
    let app = TestApp::serve(App {
        router: router(state.clone()),
        state,
    });
    let r = app
        .client()
        .get(app.url("/health/ready"))
        .header("X-Request-ID", "probe-log-1")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 503);

    let lines = logs::lines_containing("probe-log-1");
    assert!(
        lines
            .iter()
            .any(|l| l.contains("health check failed") && l.contains("correlation_id=probe-log-1")),
        "no probe log line carrying the correlation id: {lines:#?}"
    );
}
