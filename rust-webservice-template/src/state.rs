//! Per-app state (spec §3 "State"). Built once per `create_app`; cloned
//! cheaply into handlers. The start `Instant` lives in `HealthService`
//! (it owns uptime).

use crate::core::last_request::LastRequest;
use crate::core::shutdown::Shutdown;
use crate::health::ingest::IngestHealthProvider;
use crate::health::probe::HealthProbe;
use crate::health::registry::ProbeRegistry;
use crate::services::health::HealthService;
use crate::services::metrics::MetricsService;
use crate::settings::Settings;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub settings: Arc<Settings>,
    pub last_request: LastRequest,
    /// Empty in R1; R2 registers each store's probe after it connects.
    pub probes: ProbeRegistry,
    /// `NoopIngestHealth` until R3.
    pub ingest_health: Arc<dyn IngestHealthProvider>,
    pub health: Arc<HealthService>,
    /// `None` when `metrics_enabled = false`.
    pub metrics: Option<Arc<MetricsService>>,
    pub shutdown: Shutdown,
}

impl AppState {
    /// Validates `settings` first, so an `AppState` always holds valid config.
    pub fn new(
        settings: Settings,
        probes: Vec<Arc<dyn HealthProbe>>,
        ingest_health: Arc<dyn IngestHealthProvider>,
    ) -> anyhow::Result<Self> {
        settings.validate()?;
        let settings = Arc::new(settings);
        let last_request = LastRequest::new();
        let probes = ProbeRegistry::new(probes);
        let health = Arc::new(HealthService::new(
            settings.clone(),
            last_request.clone(),
            probes.clone(),
            ingest_health.clone(),
        ));
        let metrics = if settings.metrics_enabled {
            Some(Arc::new(MetricsService::new(&settings)?))
        } else {
            None
        };
        Ok(Self {
            settings,
            last_request,
            probes,
            ingest_health,
            health,
            metrics,
            shutdown: Shutdown::new(),
        })
    }
}
