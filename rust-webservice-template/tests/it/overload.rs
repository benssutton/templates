//! Production bounds of the Tower stack: requests in flight (app-wide and
//! for `POST /data/ingest`) are shed with 503 once over their limit, and a
//! request with no response after `request_timeout_seconds` is a 503. A
//! slow upload (headers plus part of a body) holds a request open.

use crate::support::{TestApp, app_settings};
use rust_template::settings::Settings;
use serde_json::Value;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Send headers announcing a 1000-byte body, and only 10 bytes of it.
async fn hold_open(app: &TestApp, path: &str, content_type: &str) -> TcpStream {
    let addr = app.base_url.trim_start_matches("http://");
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let head = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: {content_type}\r\nContent-Length: 1000\r\n\r\n0123456789"
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    // Give the server time to route the request and take its permit.
    tokio::time::sleep(Duration::from_millis(300)).await;
    stream
}

async fn read_status(stream: &mut TcpStream) -> String {
    let mut buf = vec![0_u8; 4096];
    let n = tokio::time::timeout(Duration::from_secs(10), stream.read(&mut buf))
        .await
        .expect("a response within 10 s")
        .unwrap();
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

#[tokio::test]
async fn ingest_requests_over_the_in_flight_limit_are_shed() {
    let app = TestApp::spawn(Settings {
        ingest_max_in_flight: 1,
        ..app_settings().await
    })
    .await;
    let _held = hold_open(&app, "/data/ingest", "application/vnd.apache.arrow.stream").await;
    let r = app
        .client()
        .post(app.url("/data/ingest"))
        .body(Vec::new())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 503);
    let body: Value = r.json().await.unwrap();
    assert_eq!(body, serde_json::json!({"detail": "Service Unavailable"}));
    // Other routes are not limited by the ingest bound.
    assert_eq!(app.get("/health/live").await.status(), 200);
}

#[tokio::test]
async fn requests_over_the_app_wide_limit_are_shed() {
    let app = TestApp::spawn(Settings {
        max_concurrent_requests: 1,
        ..app_settings().await
    })
    .await;
    let _held = hold_open(&app, "/config", "application/json").await;
    let r = app.get("/health/live").await;
    assert_eq!(r.status(), 503);
    assert!(r.json::<Value>().await.unwrap()["detail"].is_string());
}

#[tokio::test]
async fn a_request_without_a_response_in_time_is_503() {
    let app = TestApp::spawn(Settings {
        request_timeout_seconds: 1.0,
        ..app_settings().await
    })
    .await;
    let started = Instant::now();
    let mut held = hold_open(&app, "/config", "application/json").await;
    let text = read_status(&mut held).await;
    assert!(text.starts_with("HTTP/1.1 503"), "{text}");
    assert!(text.contains("application/json"), "{text}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
}
