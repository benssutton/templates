use crate::core::last_request::LastRequest;
use crate::core::retry::seconds;
use crate::core::system_metrics::SystemMetrics;
use crate::health::ingest::IngestHealthProvider;
use crate::health::registry::ProbeRegistry;
use crate::schemas::health::{
    AppInfo, CheckResult, DetailedStatusResponse, HealthStatusResponse, IngestHealth,
    LivenessResponse, ProbeResult, ReadinessResponse, RequestInfo, UptimeInfo,
};
use crate::settings::Settings;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct HealthService {
    settings: Arc<Settings>,
    started_at: Instant,
    last_request: LastRequest,
    probes: ProbeRegistry,
    ingest: Arc<dyn IngestHealthProvider>,
    system: SystemMetrics,
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
            system: SystemMetrics::new(), // primes CPU deltas
        }
    }

    /// For the MCP `get_health_status` tool (REST-mirroring, simple string).
    pub fn status(&self) -> HealthStatusResponse {
        HealthStatusResponse {
            status: self.settings.status.clone(),
        }
    }

    pub fn uptime_seconds(&self) -> f64 {
        self.started_at.elapsed().as_secs_f64()
    }

    pub fn liveness(&self) -> LivenessResponse {
        LivenessResponse {
            status: "alive".into(),
            uptime_seconds: round3(self.uptime_seconds()),
        }
    }

    fn probe_timeout(&self) -> Duration {
        seconds(self.settings.health_check_timeout_seconds)
    }

    async fn dependencies(&self) -> Vec<ProbeResult> {
        self.probes.run_all(self.probe_timeout()).await
    }

    fn ingest_status(&self, ingest: &IngestHealth) -> &'static str {
        if ingest.connection_state != "connected"
            || (ingest.stale && self.settings.ingest_stale_fails_readiness)
        {
            "down"
        } else {
            "up"
        }
    }

    pub async fn readiness(&self) -> ReadinessResponse {
        let deps = self.dependencies().await;
        let ingest = self.ingest.ingest_health();
        let ingest_status = self.ingest_status(&ingest);
        let all_up = deps.iter().all(|d| d.status == "up") && ingest_status == "up";

        let mut checks: Vec<CheckResult> = deps
            .into_iter()
            .map(|d| CheckResult {
                name: d.name,
                status: d.status,
                latency_ms: Some(d.latency_ms),
                error: d.error,
                ..CheckResult::default()
            })
            .collect();
        checks.push(CheckResult {
            name: "ingest".into(),
            status: ingest_status.into(),
            transport: Some(ingest.transport),
            connection_state: Some(ingest.connection_state),
            thread_alive: Some(ingest.thread_alive),
            last_batch_at: ingest.last_batch_at,
            seconds_since_last_batch: ingest.seconds_since_last_batch,
            ..CheckResult::default()
        });
        ReadinessResponse {
            status: if all_up { "ready" } else { "not_ready" }.into(),
            checks,
        }
    }

    pub async fn detailed_status(&self) -> DetailedStatusResponse {
        let dependencies = self.dependencies().await;
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
            system: self.system.snapshot(),
        }
    }
}
