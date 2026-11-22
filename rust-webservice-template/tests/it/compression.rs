//! zstd/gzip content coding through the real app. The test client decodes
//! nothing and sends no Accept-Encoding of its own, so each test sees exactly
//! the bytes and headers a client receives.

use crate::support::batches::{make_batch, serialize_batch};
use crate::support::{TestApp, app_settings};
use reqwest::StatusCode;
use reqwest::header::{ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_TYPE, HeaderMap, VARY};
use rust_template::settings::Settings;
use secrecy::SecretString;
use serde_json::{Value, json};
use std::io::{Read, Write};

const DOC: &str = "/api-doc/openapi.json";

async fn app_with(configure: impl FnOnce(&mut Settings)) -> TestApp {
    let mut settings = app_settings().await;
    configure(&mut settings);
    TestApp::spawn(settings).await
}

async fn get(
    app: &TestApp,
    method: reqwest::Method,
    path: &str,
    accept: Option<&str>,
) -> (StatusCode, HeaderMap, Vec<u8>) {
    let mut request = app.client().request(method, app.url(path));
    if let Some(accept) = accept {
        request = request.header(ACCEPT_ENCODING, accept);
    }
    let response = request.send().await.expect("request");
    let (status, headers) = (response.status(), response.headers().clone());
    (
        status,
        headers,
        response.bytes().await.expect("body").to_vec(),
    )
}

async fn post(
    app: &TestApp,
    path: &str,
    content_type: &str,
    encoding: &str,
    body: Vec<u8>,
) -> (StatusCode, HeaderMap, Value) {
    let response = app
        .client()
        .post(app.url(path))
        .header(CONTENT_TYPE, content_type)
        .header(CONTENT_ENCODING, encoding)
        .body(body)
        .send()
        .await
        .expect("POST");
    let (status, headers) = (response.status(), response.headers().clone());
    (
        status,
        headers,
        response.json().await.unwrap_or(Value::Null),
    )
}

fn encoding(headers: &HeaderMap) -> Option<&str> {
    headers.get(CONTENT_ENCODING).map(|v| v.to_str().unwrap())
}

fn varies(headers: &HeaderMap) -> bool {
    headers
        .get_all(VARY)
        .iter()
        .flat_map(|v| v.to_str().unwrap().split(','))
        .any(|v| v.trim().eq_ignore_ascii_case("accept-encoding"))
}

fn gzip(data: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

fn gunzip(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(data)
        .read_to_end(&mut out)
        .unwrap();
    out
}

fn config_body() -> Vec<u8> {
    serde_json::to_vec(&json!({"key": uuid::Uuid::new_v4().to_string(), "value": "v"})).unwrap()
}

#[tokio::test]
async fn zstd_is_preferred_and_gzip_is_the_fallback() {
    let app = app_with(|_| {}).await;
    let (_, plain_headers, plain) = get(&app, reqwest::Method::GET, DOC, None).await;
    assert_eq!(encoding(&plain_headers), None);
    assert!(varies(&plain_headers));

    let (_, headers, body) = get(&app, reqwest::Method::GET, DOC, Some("gzip, zstd")).await;
    assert_eq!(encoding(&headers), Some("zstd"));
    assert!(varies(&headers));
    assert_eq!(zstd::decode_all(body.as_slice()).unwrap(), plain);

    let (_, headers, body) = get(&app, reqwest::Method::GET, DOC, Some("gzip")).await;
    assert_eq!(encoding(&headers), Some("gzip"));
    assert_eq!(gunzip(&body), plain);
}

#[tokio::test]
async fn q_values_wildcard_and_identity_are_honoured() {
    let app = app_with(|_| {}).await;
    for (accept, expected) in [
        ("zstd;q=0.5, gzip", Some("gzip")),
        ("zstd;q=0.9996, gzip", Some("gzip")),
        ("*", Some("zstd")),
        ("*, zstd;q=0", Some("gzip")),
        ("identity", None),
        ("br", None),
    ] {
        let (_, headers, _) = get(&app, reqwest::Method::GET, DOC, Some(accept)).await;
        assert_eq!(encoding(&headers), expected, "{accept}");
    }
}

#[tokio::test]
async fn small_head_and_disabled_responses_are_not_encoded() {
    let app = app_with(|_| {}).await;
    let (_, small, _) = get(&app, reqwest::Method::GET, "/health/live", Some("zstd")).await;
    let (_, head, _) = get(&app, reqwest::Method::HEAD, DOC, Some("zstd")).await;
    assert_eq!(encoding(&small), None);
    assert_eq!(encoding(&head), None);

    let off = app_with(|s| s.compression_enabled = false).await;
    let (_, headers, _) = get(&off, reqwest::Method::GET, DOC, Some("zstd")).await;
    assert_eq!(encoding(&headers), None);
    assert!(!varies(&headers));
}

#[tokio::test]
async fn strict_mode_refuses_without_zstd_and_exempts_probes_metrics_mcp_preflight() {
    let app = app_with(|s| s.compression_require_zstd = true).await;
    for accept in [None, Some("identity"), Some("gzip"), Some("zstd;q=0")] {
        let (status, headers, body) = get(&app, reqwest::Method::GET, DOC, accept).await;
        assert_eq!(status, StatusCode::NOT_ACCEPTABLE, "{accept:?}");
        assert_eq!(encoding(&headers), None);
        assert!(varies(&headers), "{accept:?}");
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body, json!({"detail": "Accept-Encoding must allow zstd"}));
    }
    for path in ["/health/live", "/health/ready", "/metrics"] {
        let (status, _, _) = get(&app, reqwest::Method::GET, path, Some("identity")).await;
        assert_ne!(status, StatusCode::NOT_ACCEPTABLE, "{path}");
    }
    let mcp = app
        .client()
        .post(app.url("/mcp"))
        .header(ACCEPT_ENCODING, "identity")
        .header("accept", "application/json, text/event-stream")
        .json(&json!({"jsonrpc": "2.0", "id": 1, "method": "ping"}))
        .send()
        .await
        .unwrap();
    assert_ne!(mcp.status(), StatusCode::NOT_ACCEPTABLE);
    assert!(!mcp.status().is_server_error(), "{}", mcp.status());
    let preflight = app
        .client()
        .request(reqwest::Method::OPTIONS, app.url("/config"))
        .header("origin", "https://app.example.com")
        .header("access-control-request-method", "GET")
        .send()
        .await
        .unwrap();
    assert_eq!(preflight.status(), StatusCode::OK);
    // Below the usual threshold, strict mode still compresses.
    let (_, headers, _) = get(&app, reqwest::Method::GET, "/health/live", Some("zstd")).await;
    assert_eq!(encoding(&headers), Some("zstd"));
}

#[tokio::test]
async fn strict_mode_answers_406_before_authentication() {
    let app = app_with(|s| {
        s.compression_require_zstd = true;
        s.auth_enabled = true;
        s.auth_jwt_secret = SecretString::from("s".repeat(32));
    })
    .await;
    let (status, _, _) = get(&app, reqwest::Method::GET, "/config", Some("gzip")).await;
    assert_eq!(status, StatusCode::NOT_ACCEPTABLE);
}

#[tokio::test]
async fn zstd_and_gzip_request_bodies_are_accepted() {
    let app = app_with(|_| {}).await;
    for (coding, body) in [
        (
            "zstd",
            zstd::encode_all(config_body().as_slice(), 3).unwrap(),
        ),
        ("gzip", gzip(&config_body())),
        (
            " ZSTD ",
            zstd::encode_all(config_body().as_slice(), 3).unwrap(),
        ),
        ("identity", config_body()),
    ] {
        let (status, _, _) = post(&app, "/config", "application/json", coding, body).await;
        assert_eq!(status, StatusCode::CREATED, "{coding}");
    }
    let batch = serialize_batch(&make_batch(&[(1, "a", "b", "c")]));
    let (status, _, _) = post(
        &app,
        "/data/ingest",
        "application/vnd.apache.arrow.stream",
        "zstd",
        zstd::encode_all(batch.as_slice(), 3).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
}

#[tokio::test]
async fn unsupported_and_undecodable_bodies_are_refused() {
    let app = app_with(|_| {}).await;
    for coding in ["br", "deflate", "gzip, zstd"] {
        let (status, headers, body) =
            post(&app, "/config", "application/json", coding, config_body()).await;
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE, "{coding}");
        assert_eq!(headers.get(ACCEPT_ENCODING).unwrap(), "zstd, gzip");
        assert_eq!(body, json!({"detail": "Unsupported Content-Encoding"}));
        assert!(varies(&headers), "{coding}");
    }
    // Codings stacked across two header lines are refused like a list.
    let response = app
        .client()
        .post(app.url("/config"))
        .header(CONTENT_TYPE, "application/json")
        .header(CONTENT_ENCODING, "gzip")
        .header(CONTENT_ENCODING, "zstd")
        .body(config_body())
        .send()
        .await
        .expect("POST");
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(
        response.headers().get(ACCEPT_ENCODING).unwrap(),
        "zstd, gzip"
    );
    let body: Value = response.json().await.unwrap();
    assert_eq!(body, json!({"detail": "Unsupported Content-Encoding"}));
    let (status, _, body) = post(
        &app,
        "/config",
        "application/json",
        "zstd",
        b"not zstd at all".to_vec(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body, json!({"detail": "Malformed compressed request body"}));
    let (status, _, body) = post(
        &app,
        "/data/ingest",
        "application/vnd.apache.arrow.stream",
        "zstd",
        b"not zstd at all".to_vec(),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body, json!({"detail": "Malformed compressed request body"}));
    let mut truncated = zstd::encode_all(config_body().as_slice(), 3).unwrap();
    truncated.truncate(truncated.len() - 4);
    let (status, _, body) = post(&app, "/config", "application/json", "zstd", truncated).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body, json!({"detail": "Malformed compressed request body"}));
}

#[tokio::test]
async fn the_body_limit_counts_decompressed_bytes() {
    let app = app_with(|s| s.max_request_body_bytes = 1024).await;
    let bomb = zstd::encode_all(vec![0u8; 1 << 20].as_slice(), 3).unwrap();
    assert!(bomb.len() < 1024);
    let (status, _, body) = post(&app, "/config", "application/json", "zstd", bomb).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert!(body["detail"].is_string());
}
