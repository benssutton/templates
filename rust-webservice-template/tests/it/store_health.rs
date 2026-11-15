//! A store going down after startup fails readiness with a generic error.
//! Each test stops a container of its own, never a shared one (spec §5).

use crate::support::containers;
use crate::support::{app_settings, poll_ready, settings_for, TestApp};
use serde_json::Value;
use std::time::Duration;

fn check<'a>(body: &'a Value, name: &str) -> &'a Value {
    body["checks"]
        .as_array()
        .and_then(|checks| checks.iter().find(|c| c["name"] == name))
        .unwrap_or_else(|| panic!("no `{name}` check in {body}"))
}

fn assert_generic_down(body: &Value, name: &str) {
    let store = check(body, name);
    assert_eq!(store["status"], "down", "{body}");
    // The probe's own error ("unavailable") or the registry's timeout; never
    // the driver's message.
    let error = store["error"].as_str().unwrap_or_default();
    assert!(
        ["unavailable", "timeout"].contains(&error),
        "{name}: {error}"
    );
    let text = body.to_string();
    for leak in ["127.0.0.1", "localhost", "refused", "Error"] {
        assert!(!text.contains(leak), "readiness leaks `{leak}`: {text}");
    }
}

/// Python `test_dependency_down_fails_readiness` and
/// `test_health_probe_error_is_generic_not_leaky`.
#[tokio::test]
async fn redis_going_down_fails_readiness_with_a_generic_error() {
    let (pg, ch) = tokio::join!(containers::postgres(), containers::clickhouse());
    let redis = containers::redis_stack().await;
    let app = TestApp::spawn(settings_for(&pg, &ch, &redis.endpoint)).await;
    poll_ready(&app, 200, Duration::from_secs(10)).await;

    redis.stop().await;
    let body = poll_ready(&app, 503, Duration::from_secs(15)).await;
    assert_eq!(body["status"], "not_ready");
    assert_generic_down(&body, "redis");
    assert_eq!(check(&body, "postgres")["status"], "up");
    assert_eq!(check(&body, "clickhouse")["status"], "up");
}

/// Python `test_clickhouse_health_check_exception_returns_generic_error`.
#[tokio::test]
async fn clickhouse_going_down_fails_readiness_with_a_generic_error() {
    let (pg, redis) = tokio::join!(containers::postgres(), containers::redis());
    let ch = containers::clickhouse_server().await;
    let app = TestApp::spawn(settings_for(&pg, &ch.endpoint, &redis)).await;
    poll_ready(&app, 200, Duration::from_secs(10)).await;

    ch.stop().await;
    let body = poll_ready(&app, 503, Duration::from_secs(30)).await;
    assert_generic_down(&body, "clickhouse");
    assert_eq!(check(&body, "redis")["status"], "up");
}

/// A store going down mid-run turns a request that needs it into a generic
/// 500 (Deviation 8): no driver message, and never the store's credential.
/// Postgres is used because its URL carries a password.
#[tokio::test]
async fn postgres_going_down_makes_a_request_a_generic_500() {
    let (ch, redis) = tokio::join!(containers::clickhouse(), containers::redis());
    let pg = containers::Dedicated::start(containers::postgres_request(), 5432).await;
    let app = TestApp::spawn(settings_for(&pg.endpoint, &ch, &redis)).await;
    assert_eq!(app.get("/config/").await.status(), 200);

    pg.stop().await;
    let response = tokio::time::timeout(Duration::from_secs(15), app.get("/config/"))
        .await
        .expect("GET /config/ hung for 15 s after Postgres went down");
    let status = response.status();
    let headers = format!("{:?}", response.headers());
    let body = response.text().await.unwrap();
    assert_eq!(status, 500, "{body}");
    assert_eq!(body, r#"{"detail":"Internal Server Error"}"#);
    assert!(
        !headers.contains(containers::PG_PASSWORD),
        "headers leak the Postgres password: {headers}"
    );
}

/// `/metrics` exports one `dependency_up` series per store (R1's gauge,
/// now fed by real probes).
#[tokio::test]
async fn dependency_metrics_report_each_store_up() {
    let app = TestApp::spawn(app_settings().await).await;
    let text = app.get("/metrics").await.text().await.unwrap();
    for name in ["postgres", "clickhouse", "redis"] {
        let prefix = format!("dependency_up{{name=\"{name}\"}} ");
        let value = text
            .lines()
            .find_map(|line| line.strip_prefix(&prefix))
            .unwrap_or_else(|| panic!("missing {prefix}in:\n{text}"));
        assert_eq!(value.trim().parse::<f64>().unwrap(), 1.0, "{name}");

        let latency = format!("dependency_check_latency_seconds{{name=\"{name}\"}} ");
        assert!(
            text.lines().any(|line| line.starts_with(&latency)),
            "missing {latency}in:\n{text}"
        );
    }
}
