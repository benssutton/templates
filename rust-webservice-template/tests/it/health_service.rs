use crate::probes::NoopIngestHealth;
use crate::probes::{probe, PanicProbe};
use rust_template::core::last_request::LastRequest;
use rust_template::health::ingest::IngestHealthProvider;
use rust_template::health::probe::{HealthProbe, ProbeError};
use rust_template::health::registry::ProbeRegistry;
use rust_template::ingestion::ConnectionState;
use rust_template::schemas::health::{IngestHealth, Liveness, ProbeFailure, Readiness, Status};
use rust_template::services::health::HealthService;
use rust_template::settings::IngestTransport;
use rust_template::settings::Settings;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// A real IngestHealthProvider with fixed state (StreamIngestService
/// implements the same trait). Drives the readiness rules without a broker.
struct FixedIngest {
    state: ConnectionState,
    stale: bool,
}

impl IngestHealthProvider for FixedIngest {
    fn ingest_health(&self) -> IngestHealth {
        IngestHealth {
            transport: IngestTransport::Solace,
            connection_state: self.state,
            thread_alive: true,
            last_batch_at: None,
            seconds_since_last_batch: None,
            rows_ingested_total: 0,
            stale: self.stale,
            store: Default::default(),
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
    assert_eq!(live.status, Liveness::Alive);
    assert!(live.uptime_seconds >= 0.0);
}

#[tokio::test]
async fn readiness_with_no_probes_and_noop_ingest_is_ready() {
    let (svc, _) = service(
        testing(),
        ProbeRegistry::default(),
        Arc::new(NoopIngestHealth),
    );
    let ready = svc.readiness().await;
    assert_eq!(ready.status, Readiness::Ready);
    assert_eq!(ready.checks.len(), 1);
    let ingest = &ready.checks[0];
    assert_eq!(
        (ingest.name.as_str(), ingest.status),
        ("ingest", Status::Up)
    );
    assert_eq!(ingest.transport, Some(IngestTransport::Noop));
    assert_eq!(ingest.connection_state, Some(ConnectionState::Connected));
}

#[tokio::test]
async fn a_failing_probe_makes_readiness_not_ready() {
    let probes = ProbeRegistry::new(vec![
        probe("redis", Err("refused"), 0),
        probe("postgres", Ok(()), 0),
    ]);
    let (svc, _) = service(testing(), probes, Arc::new(NoopIngestHealth));
    let ready = svc.readiness().await;
    assert_eq!(ready.status, Readiness::NotReady);
    let redis = ready.checks.iter().find(|c| c.name == "redis").unwrap();
    assert_eq!(
        (redis.status, redis.error),
        (Status::Down, Some(ProbeFailure::Unavailable))
    );
}

#[tokio::test]
async fn reconnecting_ingest_is_down() {
    let ingest = Arc::new(FixedIngest {
        state: ConnectionState::Reconnecting,
        stale: false,
    });
    let (svc, _) = service(testing(), ProbeRegistry::default(), ingest);
    let ready = svc.readiness().await;
    assert_eq!(ready.status, Readiness::NotReady);
    assert_eq!(ready.checks[0].status, Status::Down);
    assert_eq!(
        ready.checks[0].connection_state,
        Some(ConnectionState::Reconnecting)
    );
}

#[tokio::test]
async fn stale_ingest_fails_readiness_only_when_configured() {
    let stale = || {
        Arc::new(FixedIngest {
            state: ConnectionState::Connected,
            stale: true,
        })
    };
    let (lenient, _) = service(testing(), ProbeRegistry::default(), stale());
    assert_eq!(lenient.readiness().await.status, Readiness::Ready);

    let strict_settings = Settings {
        ingest_stale_fails_readiness: true,
        ..testing()
    };
    let (strict, _) = service(strict_settings, ProbeRegistry::default(), stale());
    assert_eq!(strict.readiness().await.status, Readiness::NotReady);
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
    assert_eq!(status.dependencies[0].status, Status::Up);
    assert_eq!(status.ingest.transport, IngestTransport::Noop);
    assert!(status.requests.last_request_at.is_some());
    assert!(status.system.process.memory_rss_bytes > 0);
}

#[tokio::test]
async fn a_panicking_probe_makes_readiness_not_ready() {
    let probes = ProbeRegistry::new(vec![Arc::new(PanicProbe)]);
    let (svc, _) = service(testing(), probes, Arc::new(NoopIngestHealth));
    let ready = svc.readiness().await;
    assert_eq!(ready.status, Readiness::NotReady);
    assert_eq!(ready.checks[0].status, Status::Down);
}

/// Probe results are reused within `health_cache_ttl_seconds`, so a burst
/// of readiness polls and scrapes costs one round trip per store.
#[tokio::test]
async fn probe_results_are_cached_for_the_configured_ttl() {
    let counter = Arc::new(CountingProbe::default());
    let probes = ProbeRegistry::new(vec![counter.clone()]);
    let cached = Settings {
        health_cache_ttl_seconds: 60.0,
        ..testing()
    };
    let (svc, _) = service(cached, probes.clone(), Arc::new(NoopIngestHealth));
    svc.readiness().await;
    svc.detailed_status().await;
    svc.readiness().await;
    assert_eq!(counter.calls.load(Ordering::SeqCst), 1);

    let uncached = Settings {
        health_cache_ttl_seconds: 0.0,
        ..testing()
    };
    let (svc, _) = service(uncached, probes, Arc::new(NoopIngestHealth));
    svc.readiness().await;
    svc.readiness().await;
    assert_eq!(counter.calls.load(Ordering::SeqCst), 3);
}

#[derive(Default)]
struct CountingProbe {
    calls: AtomicUsize,
}

#[async_trait::async_trait]
impl HealthProbe for CountingProbe {
    fn name(&self) -> &'static str {
        "counted"
    }
    async fn check(&self) -> Result<(), ProbeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
