//! `max_request_body_bytes`: 413 with the contract's JSON body, whether the
//! size is announced (Content-Length) or only discovered while streaming.

use crate::support::{app_settings, TestApp};
use rust_template::settings::Settings;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn app(limit: usize) -> TestApp {
    TestApp::spawn(Settings {
        max_request_body_bytes: limit,
        ..app_settings().await
    })
    .await
}

/// Raw HTTP/1.1, because the server may answer and close before a client
/// has finished uploading (Windows then reports WSAECONNABORTED).
async fn raw(app: &TestApp, request: Vec<u8>) -> String {
    let addr = app.base_url.trim_start_matches("http://");
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream.write_all(&request).await.unwrap();
    let mut response = Vec::new();
    let mut buf = [0u8; 4096];
    while let Ok(n) = stream.read(&mut buf).await {
        if n == 0 {
            break;
        }
        response.extend_from_slice(&buf[..n]);
    }
    String::from_utf8_lossy(&response).into_owned()
}

fn json_body(text: &str) -> Value {
    let body = text.split("\r\n\r\n").nth(1).unwrap_or_default();
    // A chunked response frames the JSON; take the object itself.
    let start = body.find('{').unwrap_or(0);
    let end = body.rfind('}').map_or(body.len(), |i| i + 1);
    serde_json::from_str(&body[start..end]).unwrap_or(Value::Null)
}

#[tokio::test]
async fn a_body_within_the_limit_passes() {
    let app = app(1024).await;
    let (status, _) = app
        .post_json(
            "/config",
            &json!({"key": uuid::Uuid::new_v4().to_string(), "value": "v"}),
        )
        .await;
    assert_eq!(status, 201);
}

#[tokio::test]
async fn an_announced_oversized_body_is_413_json() {
    let app = app(1024).await;
    let text = raw(
        &app,
        b"POST /config HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 4096\r\nConnection: close\r\n\r\n".to_vec(),
    )
    .await;
    assert!(text.starts_with("HTTP/1.1 413"), "{text}");
    assert!(
        text.to_ascii_lowercase()
            .contains("content-type: application/json"),
        "{text}"
    );
    assert!(json_body(&text)["detail"].is_string(), "{text}");
}

/// No Content-Length: the streaming byte count must trip the limit.
#[tokio::test]
async fn a_chunked_oversized_body_is_413_json() {
    let app = app(1024).await;
    let mut request = b"POST /config HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n".to_vec();
    for _ in 0..4 {
        request.extend_from_slice(b"200\r\n"); // 0x200 = 512 bytes
        request.extend(std::iter::repeat_n(b'x', 512));
        request.extend_from_slice(b"\r\n");
    }
    request.extend_from_slice(b"0\r\n\r\n");
    let text = raw(&app, request).await;
    assert!(text.starts_with("HTTP/1.1 413"), "{text}");
    assert!(json_body(&text)["detail"].is_string(), "{text}");
}

/// The limit runs before method dispatch, so it covers every route.
#[tokio::test]
async fn the_limit_applies_to_any_route() {
    let app = app(16).await;
    let r = app
        .client()
        .post(app.url("/health/live"))
        .body(vec![b'x'; 64])
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 413);
    assert_eq!(r.headers()["content-type"], "application/json");
    assert!(r.json::<Value>().await.unwrap()["detail"].is_string());
}
