use crate::probes::NoopIngestHealth;
use crate::probes::probe;
use rust_template::core::last_request::LastRequest;
use rust_template::health::registry::ProbeRegistry;
use rust_template::services::health::HealthService;
use rust_template::services::metrics::MetricsService;
use rust_template::settings::Settings;
use std::sync::Arc;
use std::time::Duration;

fn health(probes: ProbeRegistry) -> HealthService {
    HealthService::new(
        Arc::new(Settings::default()),
        LastRequest::new(),
        probes,
        Arc::new(NoopIngestHealth),
    )
}

/// Value of the first sample line for `name` whose labels contain every
/// fragment (label order is not part of the contract).
fn sample(text: &str, name: &str, labels: &[&str]) -> Option<f64> {
    text.lines()
        .filter(|l| !l.starts_with('#'))
        .filter(|l| l.starts_with(&format!("{name}{{")) || l.starts_with(&format!("{name} ")))
        .find(|l| labels.iter().all(|frag| l.contains(frag)))
        .and_then(|l| l.rsplit(' ').next())
        .and_then(|v| v.parse().ok())
}

#[tokio::test]
async fn render_exposes_python_series_names() {
    let metrics = MetricsService::new(&Settings::default()).unwrap();
    metrics.record_request("GET", "/health/live", 200, Duration::from_millis(3));
    metrics.record_sizes("/health/live", 0, 0);
    let text = metrics
        .render(&health(ProbeRegistry::new(vec![probe(
            "postgres",
            Ok(()),
            0,
        )])))
        .await;
    for series in [
        "http_requests_total",
        "http_request_duration_seconds_bucket",
        "http_request_duration_seconds_sum",
        "http_request_duration_seconds_count",
        "http_request_duration_highr_seconds_bucket",
        "http_request_duration_highr_seconds_sum",
        "http_request_duration_highr_seconds_count",
        "http_request_size_bytes_count",
        "http_request_size_bytes_sum",
        "http_response_size_bytes_count",
        "http_response_size_bytes_sum",
        "app_info",
        "dependency_up",
        "dependency_check_latency_seconds",
        "ingest_connection_state",
        "ingest_seconds_since_last_batch",
        "app_process_memory_rss_bytes",
        "app_process_uptime_seconds",
        "system_memory_total_bytes",
        "system_boot_time_seconds",
    ] {
        assert!(text.contains(series), "missing {series} in:\n{text}");
    }
    assert!(text.contains(r#"handler="/health/live""#), "{text}");
    assert!(text.contains(r#"status="2xx""#), "{text}");
    assert_eq!(
        sample(&text, "dependency_up", &[r#"name="postgres""#]),
        Some(1.0)
    );
    assert_eq!(
        sample(
            &text,
            "ingest_connection_state",
            &[r#"transport="noop""#, r#"state="connected""#]
        ),
        Some(1.0)
    );
    assert_eq!(
        sample(&text, "ingest_connection_state", &[r#"state="down""#]),
        Some(0.0)
    );
}

#[tokio::test]
async fn seconds_since_last_batch_is_nan_before_first_batch() {
    let metrics = MetricsService::new(&Settings::default()).unwrap();
    let text = metrics.render(&health(ProbeRegistry::default())).await;
    let value = sample(&text, "ingest_seconds_since_last_batch", &[]).expect("series present");
    assert!(value.is_nan(), "expected NaN, got {value}");
}

#[tokio::test]
async fn two_apps_never_share_a_registry() {
    let a = MetricsService::new(&Settings::default()).unwrap();
    let b = MetricsService::new(&Settings::default()).unwrap();
    a.record_request("GET", "/only-in-a", 200, Duration::from_millis(1));
    let text_b = b.render(&health(ProbeRegistry::default())).await;
    assert!(!text_b.contains("/only-in-a"), "{text_b}");
}

fn le_values(text: &str, name: &str) -> Vec<String> {
    let prefix = format!("{name}_bucket{{");
    let mut seen = Vec::new();
    for line in text.lines().filter(|l| l.starts_with(&prefix)) {
        let le = line.split("le=\"").nth(1).and_then(|r| r.split('"').next());
        if let Some(le) = le
            && !seen.contains(&le.to_owned())
        {
            seen.push(le.to_owned());
        }
    }
    seen
}

#[tokio::test]
async fn histograms_use_their_configured_bucket_boundaries() {
    let metrics = MetricsService::new(&Settings::default()).unwrap();
    metrics.record_request("GET", "/x", 200, Duration::from_millis(3));
    let text = metrics.render(&health(ProbeRegistry::default())).await;
    assert_eq!(
        le_values(&text, "http_request_duration_seconds"),
        [
            "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "30",
            "+Inf"
        ]
    );
    metrics.record_sizes("/x", 10, 2_000);
    let text = metrics.render(&health(ProbeRegistry::default())).await;
    assert_eq!(
        le_values(&text, "http_response_size_bytes"),
        [
            "100",
            "1000",
            "10000",
            "100000",
            "1000000",
            "10000000",
            "100000000",
            "+Inf"
        ]
    );
    assert_eq!(
        le_values(&text, "http_request_duration_highr_seconds"),
        [
            "0.01", "0.025", "0.05", "0.075", "0.1", "0.25", "0.5", "0.75", "1", "1.5", "2", "2.5",
            "3", "3.5", "4", "4.5", "5", "7.5", "10", "30", "60", "+Inf"
        ]
    );
}

#[tokio::test]
async fn middleware_records_request_and_response_sizes() {
    use axum::{Router, middleware::from_fn_with_state, routing::post};
    let metrics = Arc::new(MetricsService::new(&Settings::default()).unwrap());
    let app = Router::new()
        .route("/echo", post(|| async { "twelve bytes" }))
        .layer(from_fn_with_state(
            metrics.clone(),
            rust_template::services::metrics::middleware,
        ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await });
    // Raw HTTP/1.1 keeps this test independent of any TLS provider setup.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut conn = tokio::net::TcpStream::connect(addr).await.unwrap();
    conn.write_all(
        b"POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 10\r\nConnection: close\r\n\r\n0123456789",
    )
    .await
    .unwrap();
    let mut raw = String::new();
    conn.read_to_string(&mut raw).await.unwrap();
    assert!(raw.starts_with("HTTP/1.1 200"), "{raw}");

    let text = metrics.render(&health(ProbeRegistry::default())).await;
    let h = [r#"handler="/echo""#];
    assert_eq!(
        sample(&text, "http_request_size_bytes_count", &h),
        Some(1.0)
    );
    assert_eq!(sample(&text, "http_request_size_bytes_sum", &h), Some(10.0));
    assert_eq!(
        sample(&text, "http_response_size_bytes_count", &h),
        Some(1.0)
    );
    assert_eq!(
        sample(&text, "http_response_size_bytes_sum", &h),
        Some(12.0)
    );
}
