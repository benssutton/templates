//! Per-app state, built once per `create_app` and cloned cheaply into
//! handlers. Services are concrete structs in `Arc`; traits exist only at
//! real seams (probes, ingest health, the consumer, the stream store).

use crate::core::auth::Authenticator;
use crate::core::last_request::LastRequest;
use crate::core::shutdown::Shutdown;
use crate::health::ingest::IngestHealthProvider;
use crate::health::probe::HealthProbe;
use crate::health::registry::ProbeRegistry;
use crate::ingestion::BatchConsumer;
use crate::persistence::Stores;
use crate::persistence::stream_store::StreamStore;
use crate::persistence::stream_store::lsm::LsmStore;
use crate::services::cache::CacheService;
use crate::services::config::ConfigService;
use crate::services::data::DataService;
use crate::services::health::HealthService;
use crate::services::metrics::MetricsService;
use crate::services::stream_ingest::StreamIngestService;
use crate::settings::Settings;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub settings: Arc<Settings>,
    pub last_request: LastRequest,
    pub health: Arc<HealthService>,
    /// `None` when `metrics_enabled = false`.
    pub metrics: Option<Arc<MetricsService>>,
    pub shutdown: Shutdown,
    /// This app's store handles; `App::close` closes them.
    pub stores: Stores,
    pub config: Arc<ConfigService>,
    pub data: Arc<DataService>,
    pub cache: Arc<CacheService>,
    /// LSM + consumer task; `App::close` closes it before the stores.
    pub ingest: Arc<StreamIngestService>,
    /// `None` when `auth_enabled = false`.
    pub auth: Option<Arc<Authenticator>>,
}

impl AppState {
    /// `settings` must already be valid (`create_app` checks them).
    /// `probes` is separate from `stores` so tests can register their own
    /// (`create_app` passes `stores.probes()`). Starts the ingest task (needs
    /// a tokio runtime) on a fresh `LsmStore`, reading from `consumer`; every
    /// fallible step runs before that, so an `Err` leaves no task behind.
    pub fn new(
        settings: Settings,
        stores: Stores,
        probes: Vec<Arc<dyn HealthProbe>>,
        consumer: Arc<dyn BatchConsumer>,
    ) -> anyhow::Result<Self> {
        let settings = Arc::new(settings);
        let last_request = LastRequest::new();
        let metrics = if settings.metrics_enabled {
            Some(Arc::new(MetricsService::new(&settings)?))
        } else {
            None
        };
        let auth = Authenticator::from_settings(&settings)?;
        let shutdown = Shutdown::new();
        let store: Arc<dyn StreamStore> = Arc::new(LsmStore::from_settings(&settings));
        let ingest = StreamIngestService::start(
            settings.clone(),
            consumer,
            store,
            shutdown.clone(),
            metrics.clone(),
        );
        let ingest_health: Arc<dyn IngestHealthProvider> = ingest.clone();
        let health = Arc::new(HealthService::new(
            settings.clone(),
            last_request.clone(),
            ProbeRegistry::new(probes),
            ingest_health,
        ));
        let config = Arc::new(ConfigService::new(stores.postgres.pool().clone()));
        let data = Arc::new(DataService::new(
            stores.clickhouse.client().clone(),
            stores.clickhouse.query_timeout(),
        ));
        let cache = Arc::new(CacheService::new(stores.redis.connection()));
        Ok(Self {
            settings,
            last_request,
            health,
            metrics,
            shutdown,
            stores,
            config,
            data,
            cache,
            auth,
            ingest,
        })
    }
}
