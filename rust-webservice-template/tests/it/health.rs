use crate::probes::probe;
use crate::support::{app_settings, TestApp};
use rust_template::app::{router, App};
use rust_template::ingestion::noop::NoopBatchConsumer;
use rust_template::persistence::Stores;
use rust_template::state::AppState;
use std::sync::Arc;

#[tokio::test]
async fn liveness_returns_alive() {
    let app = TestApp::spawn(app_settings().await).await;
    let (status, body) = app.get_json("/health/live").await;
    assert_eq!(status, 200);
    assert_eq!(body["status"], "alive");
    assert!(body["uptime_seconds"].as_f64().unwrap() >= 0.0);
}

#[tokio::test]
async fn status_reports_app_uptime_ingest_requests_and_system() {
    let app = TestApp::spawn(app_settings().await).await;
    let (status, body) = app.get_json("/health/status").await;
    assert_eq!(status, 200);
    assert_eq!(body["app"]["status"], "testing");
    assert_eq!(body["app"]["title"], "Rust Template Service");
    assert!(body["uptime"]["process_seconds"].as_f64().unwrap() >= 0.0);
    assert!(body["uptime"]["system_boot_seconds"].as_f64().unwrap() > 0.0);
    let deps = body["dependencies"].as_array().unwrap();
    let names: Vec<&str> = deps.iter().map(|d| d["name"].as_str().unwrap()).collect();
    assert_eq!(names, ["postgres", "clickhouse", "redis"]);
    assert!(deps.iter().all(|d| d["status"] == "up"), "{deps:?}");
    assert_eq!(body["ingest"]["transport"], "noop");
    assert_eq!(body["ingest"]["connection_state"], "connected");
    assert!(body["ingest"]["last_batch_at"].is_null());
    assert!(body["requests"]["last_request_at"].is_string()); // this very request
    assert!(
        body["system"]["process"]["memory_rss_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert!(
        body["system"]["host"]["memory_total_bytes"]
            .as_u64()
            .unwrap()
            > 0
    );
}

#[tokio::test]
async fn readiness_is_ready_with_an_ingest_check() {
    let app = TestApp::spawn(app_settings().await).await;
    let (status, body) = app.get_json("/health/ready").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "ready");
    let checks = body["checks"].as_array().unwrap();
    let ingest = checks.iter().find(|c| c["name"] == "ingest").unwrap();
    assert_eq!(ingest["status"], "up");
    assert_eq!(ingest["transport"], "noop");
    assert_eq!(ingest["thread_alive"], true);
    assert!(
        ingest.get("latency_ms").is_none(),
        "None fields are omitted: {ingest}"
    );
}

#[tokio::test]
async fn readiness_is_503_when_a_probe_fails_and_never_leaks_detail() {
    let settings = app_settings().await;
    let stores = Stores::connect(&settings).await.expect("stores");
    // Real stores back the services, but the registry gets only these two
    // static probes (not `stores.probes()`), so the failure is deterministic.
    let state = AppState::new(
        settings,
        stores,
        vec![
            probe("postgres", Ok(()), 0),
            probe("redis", Err("redis://:hunter2@localhost:6379 refused"), 0),
        ],
        Arc::new(NoopBatchConsumer::new()),
    )
    .unwrap();
    let app = TestApp::serve(App {
        router: router(state.clone()),
        state,
    });

    let response = app.get("/health/ready").await;
    assert_eq!(response.status(), 503);
    let text = response.text().await.unwrap();
    assert!(
        !text.contains("hunter2") && !text.contains("localhost"),
        "{text}"
    );
    let body: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(body["status"], "not_ready");
    let redis = body["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "redis")
        .unwrap()
        .clone();
    assert_eq!(redis["status"], "down");
    assert_eq!(redis["error"], "unavailable");
    assert!(redis.get("transport").is_none(), "{redis}");

    // /health/status reports the same probes, with `error: null` for the up one.
    let (_, status) = app.get_json("/health/status").await;
    let deps = status["dependencies"].as_array().unwrap();
    assert_eq!(deps[0]["name"], "postgres");
    assert!(deps[0]["error"].is_null());
    assert_eq!(deps[1]["error"], "unavailable");
}

#[tokio::test]
async fn root_returns_service_info() {
    let app = TestApp::spawn(app_settings().await).await;
    let (status, body) = app.get_json("/").await;
    assert_eq!(status, 200);
    assert_eq!(body["title"], "Rust Template Service");
    assert_eq!(body["version"], "1.0.0");
    assert_eq!(body["docs"], "/docs");
    assert_eq!(body["MCP"], "/mcp");
}

/// FastAPI's 404 body for unknown paths.
#[tokio::test]
async fn unknown_path_is_a_json_404() {
    let app = TestApp::spawn(app_settings().await).await;
    let (status, body) = app.get_json("/no/such/path").await;
    assert_eq!(status, 404);
    assert_eq!(body, serde_json::json!({ "detail": "Not Found" }));
}

/// FastAPI's 405 body (and `Allow` header) for a known path, wrong method.
#[tokio::test]
async fn wrong_method_is_a_json_405() {
    let app = TestApp::spawn(app_settings().await).await;
    let response = crate::support::client()
        .post(app.url("/health/live"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 405);
    let allow = response.headers()["allow"].to_str().unwrap().to_owned();
    assert!(allow.contains("GET"), "{allow}");
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body, serde_json::json!({ "detail": "Method Not Allowed" }));
}

#[tokio::test]
async fn stop_drains_and_returns_ok() {
    let app = TestApp::spawn(app_settings().await).await;
    assert_eq!(app.get("/health/live").await.status(), 200);
    app.stop().await.expect("clean shutdown");
}
