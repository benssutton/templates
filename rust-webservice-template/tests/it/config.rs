//! Python tests/test_config.py, plus Deviations 7 and 9.

use crate::support::{app_settings, TestApp};
use serde_json::json;

fn unique(name: &str) -> String {
    format!("{name}-{}", uuid::Uuid::new_v4().simple())
}

#[tokio::test]
async fn get_config_returns_a_list() {
    let app = TestApp::spawn(app_settings().await).await;
    let (status, body) = app.get_json("/config/").await;
    assert_eq!(status, 200);
    assert!(body.is_array(), "{body}");
}

#[tokio::test]
async fn post_config_creates_an_entry() {
    let app = TestApp::spawn(app_settings().await).await;
    let key = unique("test_creates_key");
    let (status, body) = app
        .post_json("/config/", &json!({"key": key, "value": "staging"}))
        .await;
    assert_eq!(status, 201);
    assert_eq!(body, json!({"key": key, "value": "staging"}));
}

#[tokio::test]
async fn post_config_upserts_on_duplicate_key() {
    let app = TestApp::spawn(app_settings().await).await;
    let key = unique("test_upsert_key");
    app.post_json("/config/", &json!({"key": key, "value": "staging"}))
        .await;
    let (status, body) = app
        .post_json("/config/", &json!({"key": key, "value": "production"}))
        .await;
    assert_eq!(status, 201);
    assert_eq!(body, json!({"key": key, "value": "production"}));
}

#[tokio::test]
async fn get_config_returns_all_entries() {
    let app = TestApp::spawn(app_settings().await).await;
    let key = unique("test_list_key");
    app.post_json("/config/", &json!({"key": key, "value": "value1"}))
        .await;
    let (status, body) = app.get_json("/config/").await;
    assert_eq!(status, 200);
    assert!(
        body.as_array()
            .unwrap()
            .contains(&json!({"key": key, "value": "value1"})),
        "{body}"
    );
}

/// Deviation 9: `/config` and `/config/` are the same endpoint (no redirect).
#[tokio::test]
async fn both_slash_forms_are_routed() {
    let app = TestApp::spawn(app_settings().await).await;
    let key = unique("test_slash_key");
    let (status, _) = app
        .post_json("/config", &json!({"key": key, "value": "v"}))
        .await;
    assert_eq!(status, 201);
    let (status, body) = app.get_json("/config").await;
    assert_eq!(status, 200);
    assert!(body
        .as_array()
        .unwrap()
        .contains(&json!({"key": key, "value": "v"})));
}

/// Deviation 7: FastAPI answers all of these with 422; so must we.
#[tokio::test]
async fn malformed_bodies_are_422_with_a_detail() {
    let app = TestApp::spawn(app_settings().await).await;
    let (status, body) = app
        .post_json("/config/", &json!({"key": "only-a-key"}))
        .await;
    assert_eq!(status, 422);
    assert!(body["detail"].as_str().unwrap().contains("value"), "{body}");

    let not_json = app
        .client()
        .post(app.url("/config/"))
        .header("content-type", "application/json")
        .body("{not json")
        .send()
        .await
        .unwrap();
    assert_eq!(not_json.status(), 422);

    let no_content_type = app
        .client()
        .post(app.url("/config/"))
        .body(r#"{"key":"k","value":"v"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(no_content_type.status(), 422);
}

#[tokio::test]
async fn config_reads_emit_postgres_server_timing() {
    let app = TestApp::spawn(app_settings().await).await;
    let response = app.get("/config/").await;
    assert_eq!(response.status(), 200);
    let timing = response.headers()["server-timing"]
        .to_str()
        .unwrap()
        .to_owned();
    assert!(timing.contains("postgres_config_get_all;dur="), "{timing}");
}
