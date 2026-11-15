use crate::probes::{probe, PanicProbe};
use rust_template::core::last_request::LastRequest;
use rust_template::health::ingest::{IngestHealthProvider, NoopIngestHealth};
use rust_template::health::registry::ProbeRegistry;
use rust_template::schemas::health::IngestHealth;
use rust_template::services::health::HealthService;
use rust_template::settings::Settings;
use std::sync::Arc;

/// A real IngestHealthProvider with fixed state (the R3 service implements
/// the same trait). Used to drive the readiness rules without a broker.
struct FixedIngest {
    state: &'static str,
    stale: bool,
}

impl IngestHealthProvider for FixedIngest {
    fn ingest_health(&self) -> IngestHealth {
        IngestHealth {
            transport: "solace".into(),
            connection_state: self.state.into(),
            thread_alive: true,
            last_batch_at: None,
            seconds_since_last_batch: None,
            rows_ingested_total: 0,
            stale: self.stale,
        }
    }
}

fn service(
    settings: Settings,
    probes: ProbeRegistry,
    ingest: Arc<dyn IngestHealthProvider>,
) -> (HealthService, LastRequest) {
    let last = LastRequest::new();
    (
        HealthService::new(Arc::new(settings), last.clone(), probes, ingest),
        last,
    )
}

fn testing() -> Settings {
    Settings {
        status: "testing".into(),
        ..Settings::default()
    }
}

#[tokio::test]
async fn liveness_is_alive_with_uptime() {
    let (svc, _) = service(
        testing(),
        ProbeRegistry::default(),
        Arc::new(NoopIngestHealth),
    );
    let live = svc.liveness();
    assert_eq!(live.status, "alive");
    assert!(live.uptime_seconds >= 0.0);
    assert_eq!(svc.status().status, "testing");
}

#[tokio::test]
async fn readiness_with_no_probes_and_noop_ingest_is_ready() {
    let (svc, _) = service(
        testing(),
        ProbeRegistry::default(),
        Arc::new(NoopIngestHealth),
    );
    let ready = svc.readiness().await;
    assert_eq!(ready.status, "ready");
    assert_eq!(ready.checks.len(), 1);
    let ingest = &ready.checks[0];
    assert_eq!(
        (ingest.name.as_str(), ingest.status.as_str()),
        ("ingest", "up")
    );
    assert_eq!(ingest.transport.as_deref(), Some("noop"));
    assert_eq!(ingest.connection_state.as_deref(), Some("connected"));
}

#[tokio::test]
async fn a_failing_probe_makes_readiness_not_ready() {
    let probes = ProbeRegistry::new(vec![
        probe("redis", Err("refused"), 0),
        probe("postgres", Ok(()), 0),
    ]);
    let (svc, _) = service(testing(), probes, Arc::new(NoopIngestHealth));
    let ready = svc.readiness().await;
    assert_eq!(ready.status, "not_ready");
    let redis = ready.checks.iter().find(|c| c.name == "redis").unwrap();
    assert_eq!(
        (redis.status.as_str(), redis.error.as_deref()),
        ("down", Some("unavailable"))
    );
}

#[tokio::test]
async fn reconnecting_ingest_is_down() {
    let ingest = Arc::new(FixedIngest {
        state: "reconnecting",
        stale: false,
    });
    let (svc, _) = service(testing(), ProbeRegistry::default(), ingest);
    let ready = svc.readiness().await;
    assert_eq!(ready.status, "not_ready");
    assert_eq!(ready.checks[0].status, "down");
    assert_eq!(
        ready.checks[0].connection_state.as_deref(),
        Some("reconnecting")
    );
}

#[tokio::test]
async fn stale_ingest_fails_readiness_only_when_configured() {
    let stale = || {
        Arc::new(FixedIngest {
            state: "connected",
            stale: true,
        })
    };
    let (lenient, _) = service(testing(), ProbeRegistry::default(), stale());
    assert_eq!(lenient.readiness().await.status, "ready");

    let strict_settings = Settings {
        ingest_stale_fails_readiness: true,
        ..testing()
    };
    let (strict, _) = service(strict_settings, ProbeRegistry::default(), stale());
    assert_eq!(strict.readiness().await.status, "not_ready");
}

#[tokio::test]
async fn detailed_status_reports_app_uptime_ingest_requests_and_system() {
    let probes = ProbeRegistry::new(vec![probe("postgres", Ok(()), 0)]);
    let (svc, last) = service(testing(), probes, Arc::new(NoopIngestHealth));
    assert!(svc
        .detailed_status()
        .await
        .requests
        .last_request_at
        .is_none());

    last.touch();
    let status = svc.detailed_status().await;
    assert_eq!(status.app.status, "testing");
    assert_eq!(status.app.title, "Rust Template Service");
    assert!(status.uptime.process_seconds >= 0.0);
    assert!(status.uptime.system_boot_seconds > 0.0);
    assert_eq!(status.dependencies.len(), 1);
    assert_eq!(status.dependencies[0].status, "up");
    assert_eq!(status.ingest.transport, "noop");
    assert!(status.requests.last_request_at.is_some());
    assert!(status.system.process.memory_rss_bytes > 0);
}

#[tokio::test]
async fn a_panicking_probe_makes_readiness_not_ready() {
    let probes = ProbeRegistry::new(vec![Arc::new(PanicProbe)]);
    let (svc, _) = service(testing(), probes, Arc::new(NoopIngestHealth));
    let ready = svc.readiness().await;
    assert_eq!(ready.status, "not_ready");
    assert_eq!(ready.checks[0].status, "down");
}
