//! /mcp over real HTTP. rmcp checks every request's `Host` header against
//! `mcp_allowed_hosts` (default `["localhost"]`). TestApp binds 127.0.0.1, so
//! the happy-path requests connect there and send `Host: localhost:<port>`
//! explicitly: the same check a browser's DNS-rebinding request meets, without
//! resolving `localhost` (on Windows the ::1 attempt costs seconds).
use crate::support::{app_settings, TestApp};
use rust_template::settings::Settings;
use serde_json::{json, Value};

const ACCEPT: &str = "application/json, text/event-stream";

/// `Host` header for `host` on the app's port.
fn host_header(app: &TestApp, host: &str) -> String {
    let port = app.base_url.rsplit(':').next().expect("port in base_url");
    format!("{host}:{port}")
}

/// The default allow-list's host, as a client on the published port sends it.
fn localhost(app: &TestApp) -> String {
    host_header(app, "localhost")
}

fn parse(body: &str, content_type: &str) -> Value {
    if content_type.contains("text/event-stream") {
        body.lines()
            .filter_map(|l| l.strip_prefix("data:"))
            .map(str::trim)
            .filter(|d| !d.is_empty())
            .find_map(|d| serde_json::from_str(d).ok())
            .expect("no JSON data: line in SSE response")
    } else {
        serde_json::from_str(body).expect("JSON body")
    }
}

fn initialize_body() -> Value {
    json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "test-client", "version": "0.1"}
        }
    })
}

/// POST one JSON-RPC message to `path` with the given `Host` header;
/// returns (status, session id header, body text, content type).
async fn post(
    app: &TestApp,
    path: &str,
    host: &str,
    session: Option<&str>,
    body: &Value,
) -> (u16, Option<String>, String, String) {
    let mut request = app
        .client()
        .post(app.url(path))
        .header("host", host)
        .header("accept", ACCEPT)
        .json(body);
    if let Some(id) = session {
        request = request.header("mcp-session-id", id);
    }
    let response = request.send().await.expect("POST /mcp");
    let status = response.status().as_u16();
    let session_id = response
        .headers()
        .get("mcp-session-id")
        .map(|v| v.to_str().unwrap().to_owned());
    let content_type = response
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap().to_owned())
        .unwrap_or_default();
    (
        status,
        session_id,
        response.text().await.unwrap(),
        content_type,
    )
}

/// A successful JSON-RPC exchange on `path` (`/mcp` or Python's canonical
/// `/mcp/`), addressed to `localhost`.
async fn rpc_at(
    app: &TestApp,
    path: &str,
    session: Option<&str>,
    body: &Value,
) -> (Option<String>, Value) {
    let (status, session_id, text, content_type) =
        post(app, path, &localhost(app), session, body).await;
    assert_eq!(status, 200, "{text}");
    (session_id, parse(&text, &content_type))
}

async fn rpc(app: &TestApp, session: Option<&str>, body: &Value) -> (Option<String>, Value) {
    rpc_at(app, "/mcp", session, body).await
}

/// Initialize on `path`; returns (session id, the initialize response).
async fn initialize_at(app: &TestApp, path: &str) -> (String, Value) {
    let (session, result) = rpc_at(app, path, None, &initialize_body()).await;
    assert!(
        result.get("result").is_some(),
        "initialize failed: {result}"
    );
    let session = session.expect("mcp-session-id header missing on initialize response");
    (session, result)
}

async fn initialize(app: &TestApp) -> String {
    initialize_at(app, "/mcp").await.0
}

#[tokio::test]
async fn tool_list_is_exactly_get_health_status() {
    let app = TestApp::spawn(app_settings().await).await;
    let session = initialize(&app).await;
    let (_, list) = rpc(
        &app,
        Some(&session),
        &json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}),
    )
    .await;
    let names: Vec<&str> = list["result"]["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["get_health_status"]);
}

#[tokio::test]
async fn get_health_status_mirrors_the_rest_status_field() {
    let app = TestApp::spawn(app_settings().await).await;
    let session = initialize(&app).await;
    let (_, call) = rpc(
        &app,
        Some(&session),
        &json!({"jsonrpc": "2.0", "id": 3, "method": "tools/call",
                "params": {"name": "get_health_status", "arguments": {}}}),
    )
    .await;
    assert!(call.get("error").is_none(), "{call}");
    assert_ne!(call["result"]["isError"], json!(true), "{call}");
    let content = call["result"]["content"].as_array().expect("content");
    assert_eq!(content.len(), 1);
    assert_eq!(content[0]["type"], "text");
    assert_eq!(content[0]["text"], "testing"); // HealthService::status().status
}

#[tokio::test]
async fn server_info_comes_from_the_mcp_settings() {
    let settings = Settings {
        mcp_name: "it-mcp".into(),
        mcp_instructions: "Instructions from settings.".into(),
        app_version: "9.8.7".into(),
        ..app_settings().await
    };
    let app = TestApp::spawn(settings).await;
    let (_, init) = initialize_at(&app, "/mcp").await;
    let result = &init["result"];
    assert_eq!(result["serverInfo"]["name"], "it-mcp", "{init}");
    assert_eq!(result["serverInfo"]["version"], "9.8.7", "{init}");
    assert_eq!(
        result["instructions"], "Instructions from settings.",
        "{init}"
    );
    assert!(
        result["capabilities"].get("tools").is_some(),
        "tools capability: {init}"
    );
}

/// DNS-rebinding protection: a Host outside the allow-list gets rmcp's 403 on
/// both mounts, before any session exists.
#[tokio::test]
async fn a_host_outside_the_default_allow_list_is_forbidden() {
    let app = TestApp::spawn(app_settings().await).await;
    for (path, host) in [
        ("/mcp", host_header(&app, "127.0.0.1")), // not in ["localhost"]
        ("/mcp/", host_header(&app, "evil.example")),
        ("/mcp", "evil.example".to_owned()),
    ] {
        let (status, session, text, _) = post(&app, path, &host, None, &initialize_body()).await;
        assert_eq!(status, 403, "{path} Host: {host}: {text}");
        assert!(text.contains("Host header is not allowed"), "{text}");
        assert!(session.is_none(), "no session for a forbidden host");
    }
}

/// An entry without a port matches any port (rmcp's rule), and the Host is
/// all that is checked: the request still connects to 127.0.0.1.
#[tokio::test]
async fn the_default_allow_list_admits_localhost_on_any_port() {
    let app = TestApp::spawn(app_settings().await).await;
    for host in [localhost(&app), "localhost".to_owned()] {
        let (status, session, text, _) = post(&app, "/mcp", &host, None, &initialize_body()).await;
        assert_eq!(status, 200, "Host: {host}: {text}");
        assert!(session.is_some(), "Host: {host}: no session");
    }
}

/// Compose's list (`localhost,127.0.0.1,app`): k6 and other containers reach
/// the app as `https://app`, so `Host: app` must be admitted.
#[tokio::test]
async fn a_widened_allow_list_admits_the_listed_hosts() {
    let settings = Settings {
        mcp_allowed_hosts: vec!["localhost".into(), "127.0.0.1".into(), "app".into()],
        ..app_settings().await
    };
    let app = TestApp::spawn(settings).await;
    for host in ["127.0.0.1", "app"] {
        let (status, _, text, content_type) = post(
            &app,
            "/mcp",
            &host_header(&app, host),
            None,
            &initialize_body(),
        )
        .await;
        assert_eq!(status, 200, "Host: {host}: {text}");
        let init = parse(&text, &content_type);
        assert_eq!(init["result"]["serverInfo"]["name"], "rust-template");
    }
    let (status, _, text, _) = post(
        &app,
        "/mcp",
        &host_header(&app, "evil.example"),
        None,
        &initialize_body(),
    )
    .await;
    assert_eq!(status, 403, "{text}");
}

#[tokio::test]
async fn the_allow_list_guards_only_mcp() {
    let app = TestApp::spawn(app_settings().await).await;
    let (status, body) = app.get_json("/health/live").await; // Host: 127.0.0.1:<port>
    assert_eq!(status, 200);
    assert_eq!(body["status"], "alive");
}

/// MCP's streamable HTTP owns GET/POST/DELETE on /mcp: the app's JSON 404/405
/// fallbacks must not intercept them, and /mcp must still pass through the
/// middleware stack (correlation ID; metrics labelled with the mount path).
#[tokio::test]
async fn mcp_methods_reach_rmcp_through_the_middleware_stack() {
    let app = TestApp::spawn(app_settings().await).await;
    let session = initialize(&app).await;

    // GET without a session: rmcp's own 400, not the JSON 404/405 fallback.
    let r = app
        .client()
        .get(app.url("/mcp"))
        .header("host", localhost(&app))
        .header("accept", "text/event-stream")
        .header("x-request-id", "mcp-trace")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
    assert_eq!(r.headers()["x-request-id"], "mcp-trace");

    // DELETE with the session: rmcp closes it.
    let r = app
        .client()
        .delete(app.url("/mcp"))
        .header("host", localhost(&app))
        .header("mcp-session-id", &session)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 202);

    let text = app.get("/metrics").await.text().await.unwrap();
    for method in ["POST", "GET", "DELETE"] {
        let label = format!(r#"http_requests_total{{method="{method}",handler="/mcp","#);
        assert!(
            text.lines().any(|l| l.starts_with(&label)),
            "no {label} in {text}"
        );
    }
    assert!(!text.contains(r#"handler="none""#), "{text}");
}

/// Python's canonical endpoint is `/mcp/` (FastMCP `streamable_http_path="/"`
/// mounted at `/mcp`); Rust serves both, sharing one session manager.
#[tokio::test]
async fn trailing_slash_endpoint_serves_the_full_flow() {
    let app = TestApp::spawn(app_settings().await).await;
    let (session, _) = initialize_at(&app, "/mcp/").await;
    let (_, list) = rpc_at(
        &app,
        "/mcp/",
        Some(&session),
        &json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}),
    )
    .await;
    assert_eq!(
        list["result"]["tools"][0]["name"], "get_health_status",
        "{list}"
    );
    let (_, call) = rpc_at(
        &app,
        "/mcp/",
        Some(&session),
        &json!({"jsonrpc": "2.0", "id": 3, "method": "tools/call",
                "params": {"name": "get_health_status", "arguments": {}}}),
    )
    .await;
    assert_eq!(call["result"]["content"][0]["text"], "testing", "{call}");

    // Same session manager: a session from /mcp/ is valid on /mcp.
    let (_, again) = rpc(
        &app,
        Some(&session),
        &json!({"jsonrpc": "2.0", "id": 4, "method": "tools/list"}),
    )
    .await;
    assert!(again.get("result").is_some(), "{again}");

    let text = app.get("/metrics").await.text().await.unwrap();
    assert!(
        text.lines()
            .any(|l| l.starts_with(r#"http_requests_total{method="POST",handler="/mcp/","#)),
        "{text}"
    );
}
