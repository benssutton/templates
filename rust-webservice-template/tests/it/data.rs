//! GET /data/cache reads and Server-Timing on data reads.

use crate::support::{TestApp, app_settings};

#[tokio::test]
async fn get_data_returns_the_seeded_rows() {
    let app = TestApp::spawn(app_settings().await).await;
    let (status, body) = app.get_json("/data").await;
    assert_eq!(status, 200);
    assert_eq!(body["total"], 3);
    assert_eq!(body["rows"].as_array().unwrap().len(), 3);
    assert_eq!(body["limit"], 10);
    let mut names: Vec<&str> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["name"].as_str().unwrap())
        .collect();
    names.sort();
    assert_eq!(names, ["Alpha", "Beta", "Gamma"]);
    assert!(
        body["rows"][0]["value"].is_string(),
        "value is a String column: {body}"
    );
}

#[tokio::test]
async fn limit_caps_the_page_but_not_the_total() {
    let app = TestApp::spawn(app_settings().await).await;
    let (status, body) = app.get_json("/data?limit=2").await;
    assert_eq!(status, 200);
    assert_eq!(body["rows"].as_array().unwrap().len(), 2);
    assert_eq!(body["total"], 3);
    assert_eq!(body["limit"], 2);
}

/// A limit outside 1..=100 or a non-integer limit answers 422.
#[tokio::test]
async fn limit_outside_1_to_100_or_not_an_integer_is_422() {
    let app = TestApp::spawn(app_settings().await).await;
    for bad in ["0", "101", "-5", "abc", "2.5"] {
        let (status, body) = app.get_json(&format!("/data?limit={bad}")).await;
        assert_eq!(status, 422, "limit={bad}: {body}");
        assert!(body["detail"].is_string(), "limit={bad}: {body}");
    }
}

/// A data read emits a Server-Timing header.
#[tokio::test]
async fn data_read_emits_server_timing() {
    let app = TestApp::spawn(app_settings().await).await;
    let response = app.get("/data?limit=5").await;
    assert_eq!(response.status(), 200);
    let timing = response.headers()["server-timing"]
        .to_str()
        .unwrap()
        .to_owned();
    for token in [
        "clickhouse_count;dur=",
        "clickhouse_select;dur=",
        "total;dur=",
    ] {
        assert!(timing.contains(token), "missing {token} in {timing}");
    }
}

/// `GET /data` is ordered by `id`, so pages are stable.
#[tokio::test]
async fn rows_are_ordered_by_id() {
    let app = TestApp::spawn(app_settings().await).await;
    let (status, body) = app.get_json("/data?limit=100").await;
    assert_eq!(status, 200);
    let ids: Vec<u64> = body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_u64().unwrap())
        .collect();
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    assert_eq!(ids, sorted);
}
