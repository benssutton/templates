use crate::support::{app_settings, TestApp};
use rust_template::settings::Settings;

#[tokio::test]
async fn fresh_app_preregisters_outcome_series_and_drops_the_duplicate_gauge() {
    let app = TestApp::spawn(app_settings().await).await;
    let text = app.get("/metrics").await.text().await.unwrap();
    for outcome in ["accepted", "failed", "rejected"] {
        let line = format!(r#"ingest_outcomes_total{{outcome="{outcome}"}} 0"#);
        assert!(
            text.contains(&line),
            "missing {line} in:
{text}"
        );
    }
    assert!(
        !text.lines().any(|l| l.starts_with("ingest_rows_ingested ")),
        "{text}"
    );
}

#[tokio::test]
async fn metrics_endpoint_exposes_series() {
    let app = TestApp::spawn(app_settings().await).await;
    assert_eq!(app.get("/health/live").await.status(), 200); // one request metric
    let response = app.get("/metrics").await;
    assert_eq!(response.status(), 200);
    let content_type = response.headers()["content-type"]
        .to_str()
        .unwrap()
        .to_owned();
    assert!(content_type.starts_with("text/plain"), "{content_type}");
    let text = response.text().await.unwrap();
    for series in [
        "http_request_duration_seconds",
        "http_requests_total",
        "ingest_connection_state",
        "ingest_seconds_since_last_batch",
        "app_process_memory_rss_bytes",
    ] {
        assert!(text.contains(series), "missing {series}");
    }
    assert!(text.contains(r#"handler="/health/live""#), "{text}");
}

/// The metrics middleware must run after routing (attached with
/// `Router::layer`), so `http_requests_total` carries the matched route
/// template. Unmatched paths are grouped under `handler="none"`
/// (instrumentator's `should_group_untemplated`), never the raw path.
#[tokio::test]
async fn http_requests_total_is_labelled_with_the_route_template() {
    let app = TestApp::spawn(app_settings().await).await;
    assert_eq!(app.get("/health/live").await.status(), 200);
    assert_eq!(app.get("/no/such/path").await.status(), 404);
    let text = app.get("/metrics").await.text().await.unwrap();

    let requests_total: Vec<&str> = text
        .lines()
        .filter(|l| l.starts_with("http_requests_total{"))
        .collect();
    let live = requests_total
        .iter()
        .find(|l| l.contains(r#"handler="/health/live""#))
        .unwrap_or_else(|| panic!("no /health/live series in {requests_total:?}"));
    assert!(
        live.contains(r#"method="GET""#) && live.contains(r#"status="2xx""#),
        "{live}"
    );
    assert!(live.ends_with(" 1"), "{live}");

    let unmatched = requests_total
        .iter()
        .find(|l| l.contains(r#"handler="none""#))
        .unwrap_or_else(|| panic!("no unmatched series in {requests_total:?}"));
    assert!(unmatched.contains(r#"status="4xx""#), "{unmatched}");
    assert!(
        !text.contains("/no/such/path"),
        "raw paths must not become labels"
    );
}

#[tokio::test]
async fn metrics_disabled_means_no_endpoint() {
    let app = TestApp::spawn(Settings {
        metrics_enabled: false,
        ..app_settings().await
    })
    .await;
    assert_eq!(app.get("/metrics").await.status(), 404);
}

/// Store routes are labelled with their templates too: `/config` (not
/// `/config/`, which is its own route) and `/cache/{key}` (never the key).
#[tokio::test]
async fn r2_routes_are_labelled_with_their_templates() {
    let app = TestApp::spawn(app_settings().await).await;
    let key = format!("metrics-{}", uuid::Uuid::new_v4().simple());
    assert_eq!(app.get("/config").await.status(), 200);
    assert_eq!(app.get(&format!("/cache/{key}")).await.status(), 404);
    let text = app.get("/metrics").await.text().await.unwrap();

    let requests_total: Vec<&str> = text
        .lines()
        .filter(|l| l.starts_with("http_requests_total{"))
        .collect();
    let config = requests_total
        .iter()
        .find(|l| l.contains(r#"handler="/config""#))
        .unwrap_or_else(|| panic!("no /config series in {requests_total:?}"));
    assert!(config.contains(r#"status="2xx""#), "{config}");
    let cache = requests_total
        .iter()
        .find(|l| l.contains(r#"handler="/cache/{key}""#))
        .unwrap_or_else(|| panic!("no /cache/{{key}} series in {requests_total:?}"));
    assert!(cache.contains(r#"status="4xx""#), "{cache}");
    assert!(!text.contains(&key), "raw keys must not become labels");
}
