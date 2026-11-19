//! Python tests/test_http_ingest.py: POST /data/ingest writes straight to the
//! LSM (no Delivery); GET /data/cache reads it.

use crate::support::batches::{batch_without_id, make_batch, serialize_batch};
use crate::support::logs;
use crate::support::{app_settings, TestApp};
use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use rust_template::ingestion::ipc::{batch_bytes, encode_stream};
use rust_template::settings::Settings;
use serde_json::Value;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const ARROW_STREAM: &str = "application/vnd.apache.arrow.stream";

async fn post_ipc(app: &TestApp, body: Vec<u8>, request_id: Option<&str>) -> reqwest::Response {
    let mut request = app
        .client()
        .post(app.url("/data/ingest"))
        .header("content-type", ARROW_STREAM)
        .body(body);
    if let Some(id) = request_id {
        request = request.header("X-Request-ID", id);
    }
    request.send().await.expect("POST /data/ingest")
}

async fn cache(app: &TestApp) -> Value {
    let (status, body) = app.get_json("/data/cache?limit=100").await;
    assert_eq!(status, 200, "{body}");
    body
}

fn value_of(body: &Value, id: u64) -> Option<String> {
    body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["id"] == id)
        .map(|r| r["value"].as_str().unwrap().to_owned())
}

/// Python app_settings: lsm_flush_rows=2, lsm_compaction_runs=2.
async fn settings() -> Settings {
    Settings {
        lsm_flush_rows: 2,
        lsm_compaction_runs: 2,
        ..app_settings().await
    }
}

#[tokio::test]
async fn ingest_accepts_both_arrow_media_types() {
    let app = TestApp::spawn(settings().await).await;
    for content_type in [ARROW_STREAM, "application/octet-stream"] {
        let body = serialize_batch(&make_batch(&[(101, "http", "v1", "upsert")]));
        let r = app
            .client()
            .post(app.url("/data/ingest"))
            .header("content-type", content_type)
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 202, "{content_type}");
        let body: Value = r.json().await.unwrap();
        assert_eq!(
            body,
            serde_json::json!({"accepted": true}),
            "{content_type}"
        );
    }
}

#[tokio::test]
async fn ingest_with_another_content_type_is_415() {
    let app = TestApp::spawn(settings().await).await;
    for content_type in [Some("text/plain"), Some("application/json"), None] {
        let mut request = app
            .client()
            .post(app.url("/data/ingest"))
            .body(serialize_batch(&make_batch(&[(
                102, "http", "v1", "upsert",
            )])));
        if let Some(ct) = content_type {
            request = request.header("content-type", ct);
        }
        let r = request.send().await.unwrap();
        assert_eq!(r.status(), 415, "{content_type:?}");
        let body: Value = r.json().await.unwrap();
        assert!(body["detail"].is_string(), "{body}");
    }
}

#[tokio::test]
async fn cache_is_empty_before_any_ingest_and_has_python_shape() {
    let app = TestApp::spawn(settings().await).await;
    let (status, body) = app.get_json("/data/cache").await;
    assert_eq!(status, 200);
    assert_eq!(body["total"], 0);
    assert_eq!(body["limit"], 10);
    assert_eq!(body["rows"], serde_json::json!([]));
}

#[tokio::test]
async fn an_upsert_appears_newest_wins_and_a_tombstone_removes_it() {
    let app = TestApp::spawn(settings().await).await;
    let response = post_ipc(
        &app,
        serialize_batch(&make_batch(&[(100, "http", "v1", "upsert")])),
        None,
    )
    .await;
    assert_eq!(response.status(), 202);
    assert_eq!(
        response.json::<Value>().await.unwrap(),
        serde_json::json!({ "accepted": true })
    );
    assert_eq!(value_of(&cache(&app).await, 100).as_deref(), Some("v1"));

    post_ipc(
        &app,
        serialize_batch(&make_batch(&[(100, "http", "v2", "upsert")])),
        None,
    )
    .await;
    let body = cache(&app).await;
    assert_eq!(value_of(&body, 100).as_deref(), Some("v2"));
    assert_eq!(body["rows"][0]["name"], "http");

    post_ipc(
        &app,
        serialize_batch(&make_batch(&[(100, "http", "v2", "delete")])),
        None,
    )
    .await;
    assert_eq!(value_of(&cache(&app).await, 100), None);
}

#[tokio::test]
async fn reinsert_after_delete_wins_over_http() {
    let app = TestApp::spawn(settings().await).await;
    for (value, op) in [("v1", "upsert"), ("v1", "delete"), ("v2", "upsert")] {
        let response = post_ipc(
            &app,
            serialize_batch(&make_batch(&[(500, "rk", value, op)])),
            None,
        )
        .await;
        assert_eq!(response.status(), 202);
    }
    assert_eq!(value_of(&cache(&app).await, 500).as_deref(), Some("v2"));
}

#[tokio::test]
async fn an_invalid_body_is_400() {
    let app = TestApp::spawn(settings().await).await;
    let response = post_ipc(&app, b"not arrow ipc".to_vec(), None).await;
    assert_eq!(response.status(), 400);
    assert!(response.json::<Value>().await.unwrap()["detail"].is_string());
}

#[tokio::test]
async fn an_unmergeable_batch_is_422() {
    let app = TestApp::spawn(settings().await).await;
    let response = post_ipc(&app, serialize_batch(&batch_without_id()), None).await;
    assert_eq!(response.status(), 422);
    let body: Value = response.json().await.unwrap();
    assert!(
        body["detail"]
            .as_str()
            .unwrap()
            .contains("missing required columns"),
        "{body}"
    );
}

/// Over the default 16 MiB `max_request_body_bytes`. Only the headers are
/// sent: the server answers 413 from `Content-Length` alone and then closes
/// the connection, which aborts a client still uploading 17 MiB (Windows
/// reports WSAECONNABORTED), so a real upload would make the test flaky.
#[tokio::test]
async fn an_oversized_body_is_413() {
    let app = TestApp::spawn(settings().await).await;
    let addr = app.base_url.trim_start_matches("http://");
    let request = format!(
        "POST /data/ingest HTTP/1.1
Host: {addr}
Content-Type: {ARROW_STREAM}
Content-Length: {}
Connection: close

",
        17 * 1024 * 1024
    );
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    let mut buf = [0_u8; 4096];
    // The server may reset the connection after answering: keep what we read.
    while let Ok(n) = stream.read(&mut buf).await {
        if n == 0 {
            break;
        }
        response.extend_from_slice(&buf[..n]);
    }
    let text = String::from_utf8_lossy(&response);
    assert!(
        text.starts_with("HTTP/1.1 413"),
        "unexpected response: {text}"
    );
}

/// A stream whose second batch is over `max_ingest_batch_bytes` is refused
/// whole: 413, and the first (small) batch is not written either.
#[tokio::test]
async fn an_oversized_decoded_batch_is_413_and_nothing_is_written() {
    let small = make_batch(&[(300, "small", "v1", "upsert")]);
    let rows: Vec<(i64, &str, &str, &str)> =
        (301..400).map(|id| (id, "big", "v1", "upsert")).collect();
    let big = make_batch(&rows);
    let app = TestApp::spawn(Settings {
        max_ingest_batch_bytes: batch_bytes(&small),
        ..settings().await
    })
    .await;
    let response = post_ipc(&app, encode_stream(&[small, big]).unwrap(), None).await;
    assert_eq!(response.status(), 413);
    let body: Value = response.json().await.unwrap();
    assert!(
        body["detail"]
            .as_str()
            .unwrap()
            .contains("max_ingest_batch_bytes"),
        "{body}"
    );
    assert_eq!(cache(&app).await["total"], 0, "no batch may be written");
}

/// A batch the read model cannot serve (no `name`/`value`, or a null in
/// them) is refused at ingest instead of breaking every later read.
#[tokio::test]
async fn a_batch_the_read_model_cannot_serve_is_422_and_reads_still_work() {
    let app = TestApp::spawn(settings().await).await;
    let without_value = make_batch(&[(400, "x", "v", "upsert")])
        .project(&[0, 1, 3])
        .unwrap();
    let response = post_ipc(&app, serialize_batch(&without_value), None).await;
    assert_eq!(response.status(), 422);
    let (status, body) = app.get_json("/data/cache").await;
    assert_eq!(status, 200, "{body}");
}

/// Every batch of a stream is validated before any is written: a valid
/// batch followed by an invalid one leaves nothing behind.
#[tokio::test]
async fn a_stream_with_one_invalid_batch_writes_nothing() {
    let app = TestApp::spawn(settings().await).await;
    let good = make_batch(&[(410, "good", "v1", "upsert")]);
    let schema = good.schema();
    let null_name = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![411])) as ArrayRef,
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec!["v1"])),
            Arc::new(StringArray::from(vec!["upsert"])),
        ],
    )
    .unwrap();
    let response = post_ipc(&app, encode_stream(&[good, null_name]).unwrap(), None).await;
    assert_eq!(response.status(), 422);
    assert_eq!(cache(&app).await["total"], 0);
}

/// Python test_http_ingest_propagates_request_id_to_store_write.
#[tokio::test]
async fn the_request_id_is_on_the_store_write_log_line() {
    let (logs, _guard) = logs::capture();
    let app = TestApp::spawn(settings().await).await;
    let response = post_ipc(
        &app,
        serialize_batch(&make_batch(&[(201, "http", "v1", "upsert")])),
        Some("req-xyz"),
    )
    .await;
    assert_eq!(response.status(), 202);
    assert!(
        logs.line_with(&["ingested batch", "req-xyz"]).is_some(),
        "{}",
        logs.text()
    );
}

#[tokio::test]
async fn server_timing_names_the_ingest_and_query_boundaries() {
    let app = TestApp::spawn(settings().await).await;
    let response = post_ipc(
        &app,
        serialize_batch(&make_batch(&[(1, "a", "v1", "upsert")])),
        None,
    )
    .await;
    let timing = response.headers()["server-timing"]
        .to_str()
        .unwrap()
        .to_owned();
    assert!(
        timing.contains("ingest_decode") && timing.contains("ingest_lsm_write"),
        "{timing}"
    );
    let response = app.get("/data/cache").await;
    let timing = response.headers()["server-timing"]
        .to_str()
        .unwrap()
        .to_owned();
    assert!(timing.contains("lsm_query"), "{timing}");
}

#[tokio::test]
async fn cache_limit_is_validated_like_data() {
    let app = TestApp::spawn(settings().await).await;
    for bad in ["0", "101", "abc"] {
        let (status, _) = app.get_json(&format!("/data/cache?limit={bad}")).await;
        assert_eq!(status, 422, "limit={bad}");
    }
}
