//! /mcp over real HTTP. rmcp checks every request's `Host` header against
//! `mcp_allowed_hosts` (default `["localhost"]`). TestApp binds 127.0.0.1, so
//! the happy-path requests connect there and send `Host: localhost:<port>`
//! explicitly: the same check a browser's DNS-rebinding request meets, without
//! resolving `localhost` (on Windows the ::1 attempt costs seconds).
use crate::support::{TestApp, app_settings};
use rust_template::settings::Settings;
use serde_json::{Value, json};

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

/// A successful JSON-RPC exchange on `path`,
/// addressed to `localhost`.
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

async fn call_tool(app: &TestApp, session: &str, name: &str, arguments: Value) -> Value {
    let (_, call) = rpc(
        app,
        Some(session),
        &json!({"jsonrpc": "2.0", "id": 3, "method": "tools/call",
                "params": {"name": name, "arguments": arguments}}),
    )
    .await;
    call
}

/// The single text content item of a tool result, parsed as JSON.
fn text_json(call: &Value) -> Value {
    let content = call["result"]["content"].as_array().expect("content array");
    assert_eq!(content.len(), 1, "{call}");
    assert_eq!(content[0]["type"], "text", "{call}");
    serde_json::from_str(content[0]["text"].as_str().expect("text")).expect("JSON text")
}

#[tokio::test]
async fn the_tools_mirror_the_rest_read_endpoints() {
    let app = TestApp::spawn(app_settings().await).await;
    let session = initialize(&app).await;
    let (_, list) = rpc(
        &app,
        Some(&session),
        &json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}),
    )
    .await;
    let mut names: Vec<&str> = list["result"]["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    names.sort_unstable();
    assert_eq!(
        names,
        [
            "get_cache",
            "get_cached_data",
            "get_config",
            "get_data",
            "get_health_status"
        ]
    );
}

/// The tool returns the same structured status as `GET /health/status`.
#[tokio::test]
async fn get_health_status_returns_the_detailed_status() {
    let app = TestApp::spawn(app_settings().await).await;
    let session = initialize(&app).await;
    let call = call_tool(&app, &session, "get_health_status", json!({})).await;
    assert!(call.get("error").is_none(), "{call}");
    let status = &text_json(&call);
    assert!(call["result"].get("structuredContent").is_none(), "{call}");
    assert_eq!(status["app"]["status"], "testing", "{call}");
    let deps: Vec<&str> = status["dependencies"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["name"].as_str().unwrap())
        .collect();
    assert_eq!(deps, ["postgres", "clickhouse", "redis"]);
    assert_eq!(status["ingest"]["transport"], "noop");
}

#[tokio::test]
async fn the_data_config_and_cache_tools_call_the_same_services() {
    let app = TestApp::spawn(app_settings().await).await;
    let key = format!("mcp-{}", uuid::Uuid::new_v4().simple());
    let (status, _) = app
        .post_json("/cache", &json!({"key": key, "value": {"n": 1}}))
        .await;
    assert_eq!(status, 201);
    let session = initialize(&app).await;

    let data = call_tool(&app, &session, "get_data", json!({"limit": 2})).await;
    let page = &text_json(&data);
    assert_eq!(page["total"], 3, "{data}");
    assert_eq!(page["limit"], 2, "{data}");
    let ids: Vec<u64> = page["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_u64().unwrap())
        .collect();
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    assert_eq!(ids, sorted, "ordered by id: {data}");

    let cached = call_tool(&app, &session, "get_cached_data", json!({})).await;
    assert_eq!(text_json(&cached)["limit"], 10, "{cached}");

    let config = call_tool(&app, &session, "get_config", json!({})).await;
    let (_, rest_config) = app.get_json("/config").await;
    assert!(rest_config.is_array(), "{rest_config}");
    assert_eq!(text_json(&config), rest_config, "bare array: {config}");

    let entry = call_tool(&app, &session, "get_cache", json!({"key": key})).await;
    assert_eq!(text_json(&entry)["value"], json!({"n": 1}), "{entry}");

    let missing = call_tool(
        &app,
        &session,
        "get_cache",
        json!({"key": "no-such-key-mcp"}),
    )
    .await;
    assert!(missing.get("error").is_none(), "{missing}");
    assert_eq!(missing["result"]["isError"], true, "{missing}");
    assert_eq!(
        text_json(&missing),
        json!({"detail": "Key 'no-such-key-mcp' not found"}),
        "{missing}"
    );
    let bad_limit = call_tool(&app, &session, "get_data", json!({"limit": 0})).await;
    assert!(bad_limit.get("error").is_some(), "{bad_limit}");
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

/// DNS-rebinding protection: a Host outside the allow-list is 403, with the
/// contract's JSON body, before any session exists.
#[tokio::test]
async fn a_host_outside_the_default_allow_list_is_forbidden() {
    let app = TestApp::spawn(app_settings().await).await;
    for (path, host) in [
        ("/mcp", host_header(&app, "127.0.0.1")), // not in ["localhost"]
        ("/mcp/", host_header(&app, "evil.example")),
        ("/mcp", "evil.example".to_owned()),
    ] {
        let (status, session, text, content_type) =
            post(&app, path, &host, None, &initialize_body()).await;
        assert_eq!(status, 403, "{path} Host: {host}: {text}");
        assert_eq!(content_type, "application/json");
        let body: Value = serde_json::from_str(&text).unwrap();
        assert!(
            body["detail"]
                .as_str()
                .unwrap()
                .contains("Host header is not allowed"),
            "{text}"
        );
        assert!(session.is_none(), "no session for a forbidden host");
    }
}

/// A browser on another site is refused even when the Host is allowed; an
/// Origin on an allowed host (any port) is accepted.
#[tokio::test]
async fn an_origin_outside_the_allow_list_is_forbidden() {
    let app = TestApp::spawn(app_settings().await).await;
    for (origin, expected) in [
        ("https://evil.example", 403),
        ("http://localhost:3000", 200),
        ("https://localhost", 200),
    ] {
        let r = app
            .client()
            .post(app.url("/mcp"))
            .header("host", localhost(&app))
            .header("origin", origin)
            .header("accept", ACCEPT)
            .json(&initialize_body())
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), expected, "{origin}");
        if expected == 403 {
            let body: Value = r.json().await.unwrap();
            assert!(
                body["detail"].as_str().unwrap().contains("Origin"),
                "{body}"
            );
        }
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

/// Compose's list (`localhost,127.0.0.1,app`): other containers reach the
/// app as `https://app:8443`, so `Host: app` must be admitted.
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

/// MCP's streamable HTTP owns GET/POST/DELETE on /mcp, and /mcp still
/// passes through the middleware stack (request id, JSON error bodies,
/// metrics labelled with the route).
#[tokio::test]
async fn mcp_methods_reach_rmcp_through_the_middleware_stack() {
    let app = TestApp::spawn(app_settings().await).await;
    let session = initialize(&app).await;

    // GET without a session: rmcp's own 400, as a JSON error body.
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
    assert!(r.json::<Value>().await.unwrap()["detail"].is_string());

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

/// `/mcp/` is normalised to `/mcp`: one route, one session manager.
#[tokio::test]
async fn the_trailing_slash_form_reaches_the_same_endpoint() {
    let app = TestApp::spawn(app_settings().await).await;
    let (session, _) = initialize_at(&app, "/mcp/").await;
    let (_, again) = rpc(
        &app,
        Some(&session),
        &json!({"jsonrpc": "2.0", "id": 4, "method": "tools/list"}),
    )
    .await;
    assert!(again.get("result").is_some(), "{again}");
}

/// An open SSE stream ends when the app shuts down, so it cannot hold the
/// HTTP drain open for the whole grace period.
#[tokio::test]
async fn shutdown_ends_open_event_streams_promptly() {
    let app = TestApp::spawn(app_settings().await).await;
    let session = initialize(&app).await;
    let mut stream = app
        .client()
        .get(app.url("/mcp"))
        .header("host", localhost(&app))
        .header("accept", "text/event-stream")
        .header("mcp-session-id", &session)
        .send()
        .await
        .unwrap();
    assert_eq!(stream.status(), 200);
    let started = std::time::Instant::now();
    let stopped = tokio::spawn(app.stop());
    // Read until the server ends the stream.
    while let Ok(Some(_)) = stream.chunk().await {}
    tokio::time::timeout(std::time::Duration::from_secs(5), stopped)
        .await
        .expect("the server drained within 5 s")
        .unwrap()
        .unwrap();
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
}
