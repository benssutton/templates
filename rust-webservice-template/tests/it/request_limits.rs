use axum::{body::Bytes, routing::post, Router};
use rust_template::core::request_limits;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A bare echo app behind ONLY the body-limit layer (Python test_request_limits.py `_app`).
async fn echo_app(limit: usize) -> SocketAddr {
    let router = request_limits::apply(
        Router::new().route(
            "/echo",
            post(|body: Bytes| async move { format!("got {}", body.len()) }),
        ),
        limit,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    addr
}

#[tokio::test]
async fn body_within_limit_passes() {
    let addr = echo_app(1024).await;
    let r = crate::support::client()
        .post(format!("http://{addr}/echo"))
        .body(vec![b'x'; 512])
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.text().await.unwrap(), "got 512");
}

#[tokio::test]
async fn body_over_limit_with_content_length_returns_413() {
    let addr = echo_app(1024).await;
    let r = crate::support::client()
        .post(format!("http://{addr}/echo"))
        .body(vec![b'x'; 2048]) // fixed bytes => Content-Length is set
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 413);
    assert_eq!(r.headers()["content-type"], "text/plain; charset=utf-8");
    assert_eq!(r.text().await.unwrap(), "Payload Too Large");
}

/// No Content-Length: a hand-written chunked request, so the fast path is
/// skipped and the streaming byte count must trip the limit.
#[tokio::test]
async fn chunked_body_over_limit_returns_413() {
    let addr = echo_app(1024).await;
    let mut request = b"POST /echo HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n".to_vec();
    for _ in 0..4 {
        request.extend_from_slice(b"200\r\n"); // 0x200 = 512 bytes
        request.extend(std::iter::repeat_n(b'x', 512));
        request.extend_from_slice(b"\r\n");
    }
    request.extend_from_slice(b"0\r\n\r\n");

    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream.write_all(&request).await.unwrap();
    let mut response = Vec::new();
    let mut buf = [0u8; 4096];
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
    let lower = text.to_ascii_lowercase();
    assert!(
        lower.contains("content-type: text/plain; charset=utf-8"),
        "{text}"
    );
    assert!(text.ends_with("Payload Too Large"), "{text}");
}

/// App-wide: the limit is the outermost layer, so it applies even to a
/// GET-only route (Python: MaxBodySizeMiddleware is outermost).
#[tokio::test]
async fn app_rejects_oversized_bodies_on_any_route() {
    let app = crate::support::TestApp::spawn(rust_template::settings::Settings {
        max_request_body_bytes: 16,
        ..crate::support::test_settings()
    })
    .await;
    let r = app
        .client()
        .post(app.url("/health/live"))
        .body(vec![b'x'; 64])
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 413);
    assert_eq!(r.headers()["content-type"], "text/plain; charset=utf-8");
    assert_eq!(r.text().await.unwrap(), "Payload Too Large");
}
