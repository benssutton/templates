//! `/cache`: round trips, the 404 body, the stored-null rule, the slash
//! forms and the 422 cases.

use crate::support::{app_settings, TestApp};
use serde_json::json;

fn unique(name: &str) -> String {
    format!("{name}-{}", uuid::Uuid::new_v4().simple())
}

#[tokio::test]
async fn post_cache_without_ttl() {
    let app = TestApp::spawn(app_settings().await).await;
    let key = unique("foo");
    let (status, body) = app
        .post_json("/cache/", &json!({"key": key, "value": "bar"}))
        .await;
    assert_eq!(status, 201);
    assert_eq!(
        body,
        json!({"key": key, "value": "bar", "ttl_seconds": null})
    );
}

#[tokio::test]
async fn post_cache_with_ttl() {
    let app = TestApp::spawn(app_settings().await).await;
    let key = unique("expiring");
    let (status, body) = app
        .post_json(
            "/cache/",
            &json!({"key": key, "value": "soon", "ttl_seconds": 60}),
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(
        body,
        json!({"key": key, "value": "soon", "ttl_seconds": 60})
    );

    let (status, body) = app.get_json(&format!("/cache/{key}")).await;
    assert_eq!(status, 200);
    assert_eq!(body["key"], key.as_str());
    assert_eq!(body["value"], "soon");
    let ttl = body["ttl_seconds"].as_i64().expect("ttl_seconds is set");
    assert!(0 < ttl && ttl <= 60, "{ttl}");
}

#[tokio::test]
async fn get_cache_returns_entry() {
    let app = TestApp::spawn(app_settings().await).await;
    let key = unique("foo");
    app.post_json("/cache/", &json!({"key": key, "value": "bar"}))
        .await;
    let (status, body) = app.get_json(&format!("/cache/{key}")).await;
    assert_eq!(status, 200);
    assert_eq!(
        body,
        json!({"key": key, "value": "bar", "ttl_seconds": null})
    );
}

#[tokio::test]
async fn get_cache_missing_key_returns_404_with_a_detail() {
    let app = TestApp::spawn(app_settings().await).await;
    let key = unique("nonexistent");
    let (status, body) = app.get_json(&format!("/cache/{key}")).await;
    assert_eq!(status, 404);
    assert_eq!(body, json!({"detail": format!("Key '{key}' not found")}));
}

#[tokio::test]
async fn post_cache_with_dict_value() {
    let app = TestApp::spawn(app_settings().await).await;
    let key = unique("json_dict");
    let value = json!({"nested": "object", "count": 42});
    let (status, body) = app
        .post_json("/cache/", &json!({"key": key, "value": value}))
        .await;
    assert_eq!(status, 201);
    assert_eq!(body["value"], value);
    assert!(body["ttl_seconds"].is_null());
    let (status, body) = app.get_json(&format!("/cache/{key}")).await;
    assert_eq!(status, 200);
    assert_eq!(body["value"], value);
}

#[tokio::test]
async fn post_cache_with_list_value() {
    let app = TestApp::spawn(app_settings().await).await;
    let key = unique("json_list");
    let value = json!([1, "two", {"three": 3}]);
    let (status, body) = app
        .post_json("/cache/", &json!({"key": key, "value": value}))
        .await;
    assert_eq!(status, 201);
    assert_eq!(body["value"], value);
    let (_, body) = app.get_json(&format!("/cache/{key}")).await;
    assert_eq!(body["value"], value);
}

/// RedisJSON cannot tell a stored JSON null from a missing value, so it
/// reads back as "not found".
#[tokio::test]
async fn stored_null_reads_as_404() {
    let app = TestApp::spawn(app_settings().await).await;
    let key = unique("null_value");
    let (status, _) = app
        .post_json("/cache/", &json!({"key": key, "value": null}))
        .await;
    assert_eq!(status, 201);
    let (status, _) = app.get_json(&format!("/cache/{key}")).await;
    assert_eq!(status, 404);
}

/// An empty key or a TTL below 1 is 422 (Redis EXPIRE with 0 or a negative
/// TTL would delete the key at once).
#[tokio::test]
async fn invalid_key_or_ttl_is_422_and_stores_nothing() {
    let app = TestApp::spawn(app_settings().await).await;
    let key = unique("bad_ttl");
    for payload in [
        json!({"key": key, "value": 1, "ttl_seconds": 0}),
        json!({"key": key, "value": 1, "ttl_seconds": -1}),
        json!({"key": "", "value": 1}),
    ] {
        let (status, body) = app.post_json("/cache/", &payload).await;
        assert_eq!(status, 422, "{payload}: {body}");
    }
    let (status, _) = app.get_json(&format!("/cache/{key}")).await;
    assert_eq!(status, 404);
}

#[tokio::test]
async fn post_without_trailing_slash_is_routed_and_missing_value_is_422() {
    let app = TestApp::spawn(app_settings().await).await;
    let key = unique("slash");
    let (status, _) = app
        .post_json("/cache", &json!({"key": key, "value": 1}))
        .await;
    assert_eq!(status, 201);
    let (status, body) = app.post_json("/cache/", &json!({"key": key})).await;
    assert_eq!(status, 422);
    assert!(body["detail"].as_str().unwrap().contains("value"), "{body}");
}
