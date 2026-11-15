//! Per-app state (spec §3 "State"). Built once per `create_app`; cloned
//! cheaply into handlers. The start `Instant` lives in `HealthService`
//! (it owns uptime).

use crate::core::last_request::LastRequest;
use crate::core::shutdown::Shutdown;
use crate::health::ingest::IngestHealthProvider;
use crate::health::probe::HealthProbe;
use crate::health::registry::ProbeRegistry;
use crate::persistence::Stores;
use crate::services::cache::CacheService;
use crate::services::config::ConfigService;
use crate::services::data::DataService;
use crate::services::health::HealthService;
use crate::services::metrics::MetricsService;
use crate::settings::Settings;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub settings: Arc<Settings>,
    pub last_request: LastRequest,
    /// `create_app` registers the three store probes (`Stores::probes`).
    pub probes: ProbeRegistry,
    /// `NoopIngestHealth` until R3.
    pub ingest_health: Arc<dyn IngestHealthProvider>,
    pub health: Arc<HealthService>,
    /// `None` when `metrics_enabled = false`.
    pub metrics: Option<Arc<MetricsService>>,
    pub shutdown: Shutdown,
    /// This app's store handles; `App::close` closes them.
    pub stores: Stores,
    pub config: Arc<ConfigService>,
    pub data: Arc<DataService>,
    pub cache: Arc<CacheService>,
}

impl AppState {
    /// Validates `settings` first, so an `AppState` always holds valid config.
    /// `probes` is separate from `stores` so tests can register their own
    /// probes (`create_app` passes `stores.probes()`).
    pub fn new(
        settings: Settings,
        stores: Stores,
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
        let config = Arc::new(ConfigService::new(stores.postgres.pool().clone()));
        let data = Arc::new(DataService::new(stores.clickhouse.client().clone()));
        let cache = Arc::new(CacheService::new(stores.redis.connection()));
        Ok(Self {
            settings,
            last_request,
            probes,
            ingest_health,
            health,
            metrics,
            shutdown: Shutdown::new(),
            stores,
            config,
            data,
            cache,
        })
    }
}
