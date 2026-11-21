//! Request ids: adopted only when 1-128 chars of `[A-Za-z0-9._-]`, otherwise
//! replaced; echoed on every response; carried by every log line of the
//! request, including the access log.

use crate::probes::probe;
use crate::support::{TestApp, app_settings, logs};
use rust_template::app::{App, router};
use rust_template::ingestion::noop::NoopBatchConsumer;
use rust_template::persistence::Stores;
use rust_template::settings::Settings;
use rust_template::state::AppState;
use std::sync::Arc;

async fn id_for(app: &TestApp, inbound: Option<&str>) -> String {
    let mut request = app.client().get(app.url("/"));
    if let Some(id) = inbound {
        request = request.header("X-Request-ID", id);
    }
    let response = request.send().await.unwrap();
    response.headers()["x-request-id"]
        .to_str()
        .unwrap()
        .to_owned()
}

fn is_generated(id: &str) -> bool {
    id.len() == 32 && id.chars().all(|c| c.is_ascii_hexdigit())
}

#[tokio::test]
async fn an_acceptable_inbound_id_is_echoed() {
    let app = TestApp::spawn(app_settings().await).await;
    assert_eq!(id_for(&app, Some("trace-42")).await, "trace-42");
    let longest = "a".repeat(128);
    assert_eq!(id_for(&app, Some(&longest)).await, longest);
}

#[tokio::test]
async fn an_unacceptable_inbound_id_is_replaced() {
    let app = TestApp::spawn(app_settings().await).await;
    for bad in ["has space", "semi;colon", &"a".repeat(129)] {
        let id = id_for(&app, Some(bad)).await;
        assert!(is_generated(&id), "{bad:?} -> {id}");
    }
}

#[tokio::test]
async fn a_32_hex_id_is_generated_when_absent() {
    let app = TestApp::spawn(app_settings().await).await;
    let id = id_for(&app, None).await;
    assert!(is_generated(&id), "{id}");
    assert_ne!(id, id_for(&app, None).await, "each request gets a fresh id");
}

#[tokio::test]
async fn header_name_is_configurable() {
    let app = TestApp::spawn(Settings {
        correlation_id_header: "X-Correlation-ID".into(),
        ..app_settings().await
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
    let app = TestApp::spawn(app_settings().await).await;
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
    assert_eq!(body, serde_json::json!({"detail": "Not Found"}));
}

/// One access-log line per request, inside the request span, with the
/// status and latency.
#[tokio::test]
async fn the_access_log_line_carries_id_status_and_latency() {
    logs::ensure_installed();
    let app = TestApp::spawn(app_settings().await).await;
    app.client()
        .get(app.url("/health/live"))
        .header("X-Request-ID", "access-log-1")
        .send()
        .await
        .unwrap();
    let lines = logs::lines_containing("access-log-1");
    assert!(
        lines.iter().any(|l| l.contains("request completed")
            && l.contains("status=200")
            && l.contains("latency_ms=")
            && l.contains("request_id=access-log-1")),
        "no access log line: {lines:#?}"
    );
}

/// Through the real app: a failing probe's log line (emitted on a spawned
/// task) still carries the request's id.
#[tokio::test]
async fn probe_failure_logs_carry_the_request_id() {
    logs::ensure_installed();
    let settings = Settings {
        health_cache_ttl_seconds: 0.0,
        ..app_settings().await
    };
    let stores = Stores::connect(&settings).await.expect("stores");
    // Real stores back the services; the registry gets only the failing
    // static probe (not `stores.probes()`), so the failure is deterministic.
    let state = AppState::new(
        settings,
        stores,
        vec![probe("redis", Err("refused"), 0)],
        Arc::new(NoopBatchConsumer::new()),
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
            .any(|l| l.contains("health check failed") && l.contains("request_id=probe-log-1")),
        "no probe log line carrying the request id: {lines:#?}"
    );
}
