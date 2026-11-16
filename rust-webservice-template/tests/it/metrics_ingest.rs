use rust_template::core::last_request::LastRequest;
use rust_template::health::ingest::NoopIngestHealth;
use rust_template::health::registry::ProbeRegistry;
use rust_template::services::health::HealthService;
use rust_template::services::metrics::MetricsService;
use rust_template::settings::Settings;
use std::sync::Arc;

fn health() -> HealthService {
    HealthService::new(
        Arc::new(Settings::default()),
        LastRequest::new(),
        ProbeRegistry::default(),
        Arc::new(NoopIngestHealth),
    )
}

#[tokio::test]
async fn settlement_and_row_counters_are_exported() {
    let metrics = MetricsService::new(&Settings::default()).unwrap();
    metrics.record_settlement("accepted");
    metrics.record_settlement("accepted");
    metrics.record_settlement("rejected");
    metrics.record_ingested_rows(5);
    let text = metrics.render(&health()).await;
    assert!(
        text.contains(r#"ingest_deliveries_total{outcome="accepted"} 2"#),
        "{text}"
    );
    assert!(
        text.contains(r#"ingest_deliveries_total{outcome="rejected"} 1"#),
        "{text}"
    );
    assert!(text.contains("ingest_rows_total 5"), "{text}");
}
