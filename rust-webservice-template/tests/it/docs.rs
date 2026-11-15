use crate::support::{test_settings, TestApp};

#[tokio::test]
async fn swagger_ui_is_served() {
    let app = TestApp::spawn(test_settings()).await;
    let status = app.get("/docs").await.status().as_u16();
    // reqwest follows the /docs -> /docs/ redirect; either way it must not 404.
    assert!(status == 200 || status == 303, "got {status}");
}

#[tokio::test]
async fn openapi_lists_the_r1_paths() {
    let app = TestApp::spawn(test_settings()).await;
    let (status, doc) = app.get_json("/api-doc/openapi.json").await;
    assert_eq!(status, 200);
    for path in [
        "/",
        "/health/live",
        "/health/ready",
        "/health/status",
        "/metrics",
    ] {
        assert!(doc["paths"].get(path).is_some(), "missing {path}");
    }
    assert!(
        doc["paths"].get("/data/shape").is_none(),
        "/data/shape must be removed"
    );
    assert!(doc["components"]["schemas"]
        .get("DetailedStatusResponse")
        .is_some());
}

#[tokio::test]
async fn openapi_passes_through_the_middleware_stack() {
    let app = TestApp::spawn(test_settings()).await;
    let r = app
        .client()
        .get(app.url("/api-doc/openapi.json"))
        .header("x-request-id", "docs-trace")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.headers()["x-request-id"], "docs-trace");
    let text = app.get("/metrics").await.text().await.unwrap();
    assert!(
        text.contains(r#"handler="/api-doc/openapi.json""#),
        "{text}"
    );
}
