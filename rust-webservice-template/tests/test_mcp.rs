use axum_test::TestServer;
use rust_template::build_app;

mod common;

fn parse_mcp_response(body: &[u8], content_type: &str) -> serde_json::Value {
    if content_type.contains("text/event-stream") {
        let text = std::str::from_utf8(body).unwrap();
        text.lines()
            .filter(|l| l.starts_with("data:"))
            .map(|l| l["data:".len()..].trim())
            .filter(|data| !data.is_empty())
            .find_map(|data| serde_json::from_str(data).ok())
            .expect("no valid JSON data: line found in SSE response")
    } else {
        serde_json::from_slice(body).unwrap()
    }
}

#[tokio::test]
async fn test_mcp_tool_list() {
    let app = build_app(common::test_state());
    let server = TestServer::new(app);

    // Step 1: initialize session
    let init_body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "test-client", "version": "0.1"}
        }
    });

    let init_response = server
        .post("/mcp")
        .add_header(
            axum::http::HeaderName::from_static("content-type"),
            axum::http::HeaderValue::from_static("application/json"),
        )
        .add_header(
            axum::http::HeaderName::from_static("accept"),
            axum::http::HeaderValue::from_static("application/json, text/event-stream"),
        )
        .json(&init_body)
        .await;

    assert_eq!(init_response.status_code(), axum::http::StatusCode::OK);

    let session_id = init_response
        .headers()
        .get("mcp-session-id")
        .expect("mcp-session-id header missing on initialize response")
        .to_str()
        .unwrap()
        .to_string();

    let ct = init_response
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let init_bytes = init_response.as_bytes().to_vec();
    let init_result = parse_mcp_response(&init_bytes, &ct);
    assert!(
        init_result.get("result").is_some(),
        "initialize result missing"
    );

    // Step 2: list tools
    let list_body = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "tools/list",
        "id": 2
    });

    let list_response = server
        .post("/mcp")
        .add_header(
            axum::http::HeaderName::from_static("content-type"),
            axum::http::HeaderValue::from_static("application/json"),
        )
        .add_header(
            axum::http::HeaderName::from_static("accept"),
            axum::http::HeaderValue::from_static("application/json, text/event-stream"),
        )
        .add_header(
            axum::http::HeaderName::from_static("mcp-session-id"),
            axum::http::HeaderValue::from_str(&session_id).unwrap(),
        )
        .json(&list_body)
        .await;

    assert_eq!(list_response.status_code(), axum::http::StatusCode::OK);

    let ct2 = list_response
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let list_bytes = list_response.as_bytes().to_vec();
    let list_result = parse_mcp_response(&list_bytes, &ct2);

    let tool_names: Vec<&str> = list_result["result"]["tools"]
        .as_array()
        .expect("tools array missing")
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();

    assert!(
        tool_names.contains(&"get_health_service_tool"),
        "get_health_service_tool not in tool list: {tool_names:?}"
    );
    assert!(
        tool_names.contains(&"get_data_service_tool"),
        "get_data_service_tool not in tool list: {tool_names:?}"
    );
}

/// Sends a JSON-RPC request to /mcp (with optional session id) and returns the
/// response headers' session id (if any) together with the parsed JSON body.
async fn mcp_post(
    server: &TestServer,
    session_id: Option<&str>,
    body: &serde_json::Value,
) -> (Option<String>, serde_json::Value) {
    let mut req = server
        .post("/mcp")
        .add_header(
            axum::http::HeaderName::from_static("content-type"),
            axum::http::HeaderValue::from_static("application/json"),
        )
        .add_header(
            axum::http::HeaderName::from_static("accept"),
            axum::http::HeaderValue::from_static("application/json, text/event-stream"),
        );
    if let Some(id) = session_id {
        req = req.add_header(
            axum::http::HeaderName::from_static("mcp-session-id"),
            axum::http::HeaderValue::from_str(id).unwrap(),
        );
    }
    let response = req.json(body).await;
    assert_eq!(response.status_code(), axum::http::StatusCode::OK);

    let sid = response
        .headers()
        .get("mcp-session-id")
        .map(|v| v.to_str().unwrap().to_string());
    let ct = response
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let bytes = response.as_bytes().to_vec();
    (sid, parse_mcp_response(&bytes, &ct))
}

/// Returns the text of the single text content item in a tools/call result.
fn tool_call_text(result: &serde_json::Value) -> &str {
    assert!(
        result.get("error").is_none(),
        "tools/call returned error: {result}"
    );
    assert_ne!(
        result["result"]["isError"],
        serde_json::Value::Bool(true),
        "tool reported isError: {result}"
    );
    let content = result["result"]["content"]
        .as_array()
        .expect("tools/call content array missing");
    assert_eq!(
        content.len(),
        1,
        "expected exactly one content item: {result}"
    );
    assert_eq!(content[0]["type"], "text");
    content[0]["text"].as_str().expect("content text missing")
}

#[tokio::test]
async fn test_mcp_tool_call() {
    let app = build_app(common::test_state());
    let server = TestServer::new(app);

    let init_body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "test-client", "version": "0.1"}
        }
    });
    let (session_id, init_result) = mcp_post(&server, None, &init_body).await;
    let session_id = session_id.expect("mcp-session-id header missing on initialize response");
    assert!(
        init_result.get("result").is_some(),
        "initialize result missing"
    );

    // Health tool returns the configured status string.
    let health_body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {"name": "get_health_service_tool", "arguments": {}}
    });
    let (_, health_result) = mcp_post(&server, Some(&session_id), &health_body).await;
    assert_eq!(health_result["id"], 2);
    assert_eq!(tool_call_text(&health_result), "testing");

    // Data tool returns the shape of the test data set as JSON.
    let data_body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {"name": "get_data_service_tool", "arguments": {}}
    });
    let (_, data_result) = mcp_post(&server, Some(&session_id), &data_body).await;
    assert_eq!(data_result["id"], 3);
    let shape: serde_json::Value =
        serde_json::from_str(tool_call_text(&data_result)).expect("data tool did not return JSON");
    assert_eq!(shape, serde_json::json!({"height": 3, "width": 2}));
}
