use crate::support::{app_settings, TestApp};
use serde_json::{json, Value};

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

/// POST one JSON-RPC message to /mcp; returns (session id header, parsed body).
async fn rpc(app: &TestApp, session: Option<&str>, body: &Value) -> (Option<String>, Value) {
    rpc_at(app, "/mcp", session, body).await
}

/// POST one JSON-RPC message to `path` (`/mcp` or Python's canonical `/mcp/`).
async fn rpc_at(
    app: &TestApp,
    path: &str,
    session: Option<&str>,
    body: &Value,
) -> (Option<String>, Value) {
    let mut request = app
        .client()
        .post(app.url(path))
        .header("accept", "application/json, text/event-stream")
        .json(body);
    if let Some(id) = session {
        request = request.header("mcp-session-id", id);
    }
    let response = request.send().await.unwrap();
    assert_eq!(response.status(), 200);
    let session_id = response
        .headers()
        .get("mcp-session-id")
        .map(|v| v.to_str().unwrap().to_owned());
    let content_type = response.headers()["content-type"]
        .to_str()
        .unwrap()
        .to_owned();
    let text = response.text().await.unwrap();
    (session_id, parse(&text, &content_type))
}

async fn initialize(app: &TestApp) -> String {
    initialize_at(app, "/mcp").await
}

async fn initialize_at(app: &TestApp, path: &str) -> String {
    let (session, result) = rpc_at(
        app,
        path,
        None,
        &json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": {"name": "test-client", "version": "0.1"}
            }
        }),
    )
    .await;
    assert!(
        result.get("result").is_some(),
        "initialize failed: {result}"
    );
    session.expect("mcp-session-id header missing on initialize response")
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
    assert_eq!(content[0]["text"], "testing");
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
    let session = initialize_at(&app, "/mcp/").await;
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
