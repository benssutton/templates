use crate::support::{TestApp, app_settings};

#[tokio::test]
async fn swagger_ui_is_served() {
    let app = TestApp::spawn(app_settings().await).await;
    let status = app.get("/docs").await.status().as_u16();
    // reqwest follows the /docs -> /docs/ redirect; either way it must not 404.
    assert!(status == 200 || status == 303, "got {status}");
}

#[tokio::test]
async fn openapi_lists_the_service_paths() {
    let app = TestApp::spawn(app_settings().await).await;
    let (status, doc) = app.get_json("/api-doc/openapi.json").await;
    assert_eq!(status, 200);
    for path in [
        "/",
        "/health/live",
        "/health/ready",
        "/health/status",
        "/metrics",
        "/mcp",
        "/data/cache",
        "/data/ingest",
    ] {
        assert!(doc["paths"].get(path).is_some(), "missing {path}");
    }
    assert!(
        doc["components"]["schemas"]
            .get("DetailedStatusResponse")
            .is_some()
    );
}

#[tokio::test]
async fn openapi_passes_through_the_middleware_stack() {
    let app = TestApp::spawn(app_settings().await).await;
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

#[tokio::test]
async fn openapi_lists_methods_parameters_and_schemas() {
    let app = TestApp::spawn(app_settings().await).await;
    let (status, doc) = app.get_json("/api-doc/openapi.json").await;
    assert_eq!(status, 200);
    for (path, method) in [
        ("/config", "get"),
        ("/config", "post"),
        ("/data", "get"),
        ("/cache", "post"),
        ("/cache/{key}", "get"),
        ("/mcp", "post"),
        ("/mcp", "get"),
        ("/mcp", "delete"),
    ] {
        assert!(
            doc["paths"][path].get(method).is_some(),
            "missing {method} {path}"
        );
    }
    let limit = &doc["paths"]["/data"]["get"]["parameters"][0];
    assert_eq!(limit["name"], "limit");
    assert_eq!(limit["in"], "query");
    for schema in [
        "ConfigSetRequest",
        "ConfigEntry",
        "DataRowsResponse",
        "DataRowResponse",
        "CacheSetRequest",
        "CacheEntry",
        "ErrorResponse",
    ] {
        assert!(
            doc["components"]["schemas"].get(schema).is_some(),
            "missing schema {schema}"
        );
    }
}

/// Error responses are documented with the shared error body, and the
/// title and version come from settings.
#[tokio::test]
async fn openapi_documents_errors_and_takes_identity_from_settings() {
    let app = TestApp::spawn(rust_template::settings::Settings {
        app_title: "Docs IT".into(),
        app_version: "3.2.1".into(),
        ..app_settings().await
    })
    .await;
    let (_, doc) = app.get_json("/api-doc/openapi.json").await;
    assert_eq!(doc["info"]["title"], "Docs IT");
    assert_eq!(doc["info"]["version"], "3.2.1");
    let unavailable = &doc["paths"]["/data"]["get"]["responses"]["503"];
    let schema = unavailable["content"]["application/json"]["schema"]["$ref"]
        .as_str()
        .unwrap_or_default();
    assert!(schema.ends_with("/ErrorResponse"), "{unavailable}");
    assert!(
        doc["paths"]["/data/ingest"]["post"]["responses"]
            .get("413")
            .is_some()
    );
}
