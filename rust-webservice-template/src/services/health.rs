//! Liveness, readiness and detailed status. Dependency probe results are
//! reused for `health_cache_ttl_seconds`, so frequent readiness polls and
//! Prometheus scrapes do not each open a round trip to every store; one
//! caller refreshes while the others wait for its result.

use crate::core::last_request::LastRequest;
use crate::core::retry::seconds;
use crate::core::system_metrics::SystemMetrics;
use crate::health::ingest::IngestHealthProvider;
use crate::health::registry::ProbeRegistry;
use crate::ingestion::ConnectionState;
use crate::schemas::health::{
    AppInfo, CheckResult, DetailedStatusResponse, IngestHealth, Liveness, LivenessResponse,
    ProbeResult, Readiness, ReadinessResponse, RequestInfo, Status, SystemSnapshot, UptimeInfo,
};
use crate::settings::Settings;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

pub struct HealthService {
    settings: Arc<Settings>,
    started_at: Instant,
    last_request: LastRequest,
    probes: ProbeRegistry,
    ingest: Arc<dyn IngestHealthProvider>,
    system: Arc<SystemMetrics>,
    cached: Mutex<Option<(Instant, Vec<ProbeResult>)>>,
}

fn round3(x: f64) -> f64 {
    (x * 1000.0).round() / 1000.0
}

impl HealthService {
    pub fn new(
        settings: Arc<Settings>,
        last_request: LastRequest,
        probes: ProbeRegistry,
        ingest: Arc<dyn IngestHealthProvider>,
    ) -> Self {
        Self {
            settings,
            started_at: Instant::now(),
            last_request,
            probes,
            ingest,
            system: Arc::new(SystemMetrics::new()), // primes CPU deltas
            cached: Mutex::new(None),
        }
    }

    pub fn uptime_seconds(&self) -> f64 {
        self.started_at.elapsed().as_secs_f64()
    }

    pub fn liveness(&self) -> LivenessResponse {
        LivenessResponse {
            status: Liveness::Alive,
            uptime_seconds: round3(self.uptime_seconds()),
        }
    }

    fn cache_ttl(&self) -> Duration {
        seconds(self.settings.health_cache_ttl_seconds)
    }

    /// Probe results no older than `health_cache_ttl_seconds`.
    pub async fn dependencies(&self) -> Vec<ProbeResult> {
        let mut cached = self.cached.lock().await;
        if let Some((at, results)) = cached.as_ref()
            && at.elapsed() < self.cache_ttl()
        {
            return results.clone();
        }
        let timeout = seconds(self.settings.health_check_timeout_seconds);
        let results = self.probes.run_all(timeout).await;
        *cached = Some((Instant::now(), results.clone()));
        results
    }

    fn ingest_status(&self, ingest: &IngestHealth) -> Status {
        if ingest.connection_state != ConnectionState::Connected
            || ingest.store.poisoned
            || (ingest.stale && self.settings.ingest_stale_fails_readiness)
        {
            Status::Down
        } else {
            Status::Up
        }
    }

    pub async fn readiness(&self) -> ReadinessResponse {
        let deps = self.dependencies().await;
        let ingest = self.ingest.ingest_health();
        let ingest_status = self.ingest_status(&ingest);
        let all_up = deps.iter().all(|d| d.status == Status::Up) && ingest_status == Status::Up;
        let mut checks: Vec<CheckResult> = deps.into_iter().map(CheckResult::probe).collect();
        checks.push(CheckResult::ingest(ingest_status, ingest));
        ReadinessResponse {
            status: if all_up {
                Readiness::Ready
            } else {
                Readiness::NotReady
            },
            checks,
        }
    }

    /// sysinfo reads /proc (or the OS equivalent) synchronously, so the
    /// snapshot runs on the blocking pool.
    async fn system_snapshot(&self) -> SystemSnapshot {
        let system = self.system.clone();
        match tokio::task::spawn_blocking(move || system.snapshot()).await {
            Ok(snapshot) => snapshot,
            Err(err) => {
                tracing::error!(error = %err, "system metrics task failed");
                SystemSnapshot::default()
            }
        }
    }

    pub async fn detailed_status(&self) -> DetailedStatusResponse {
        let (dependencies, system) = tokio::join!(self.dependencies(), self.system_snapshot());
        DetailedStatusResponse {
            app: AppInfo {
                title: self.settings.app_title.clone(),
                version: self.settings.app_version.clone(),
                status: self.settings.status.clone(),
            },
            uptime: UptimeInfo {
                process_seconds: round3(self.uptime_seconds()),
                system_boot_seconds: SystemMetrics::boot_time_seconds(),
            },
            dependencies,
            ingest: self.ingest.ingest_health(),
            requests: RequestInfo {
                last_request_at: self.last_request.get(),
            },
            system,
        }
    }
}
