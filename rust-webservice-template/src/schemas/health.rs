//! Health response shapes. Statuses are enums serialised as the lowercase
//! strings of the shared contract.

use crate::ingestion::ConnectionState;
use crate::settings::IngestTransport;
use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;

/// One dependency or check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Up,
    Down,
}

/// Why a probe is down. Never a raw driver message (it may hold a credential).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum ProbeFailure {
    Unavailable,
    Timeout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum Liveness {
    Alive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Readiness {
    Ready,
    NotReady,
}

/// Result of a single dependency health probe.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ProbeResult {
    pub name: String,
    pub status: Status,
    pub latency_ms: f64,
    pub error: Option<ProbeFailure>,
}

/// The in-memory stream store's size and capacity state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, ToSchema)]
pub struct StreamStoreStats {
    /// Rows physically held across every part, dead rows included.
    pub rows: u64,
    /// Live rows: what `GET /data/cache` serves.
    pub live_rows: u64,
    /// Parts held across every layer.
    pub parts: u64,
    /// The cap on `rows`.
    pub max_rows: u64,
    /// A message would have taken `rows` over `max_rows`, so the store refuses
    /// every new message until it is rebuilt. Reads still serve the rows
    /// received to date, so readiness stays up.
    pub full: bool,
    /// Non-empty batches refused because the store is full.
    pub refused_batches_total: u64,
    /// Merges that failed; each is retried once its layer's part count has
    /// doubled.
    pub merge_failures_total: u64,
    /// A write panicked, so the store refuses every later write; readiness
    /// fails until the instance is restarted.
    pub poisoned: bool,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct IngestHealth {
    pub transport: IngestTransport,
    pub connection_state: ConnectionState,
    /// True while the ingest task runs (the field name is part of the shared contract).
    pub thread_alive: bool,
    pub last_batch_at: Option<DateTime<Utc>>,
    pub seconds_since_last_batch: Option<f64>,
    pub rows_ingested_total: u64,
    pub stale: bool,
    pub store: StreamStoreStats,
}

/// One entry of the flat `/health/ready` checks array. `None` fields are
/// omitted, so a store check and the ingest check share one shape.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct CheckResult {
    pub name: String,
    pub status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transport: Option<IngestTransport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_state: Option<ConnectionState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_alive: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_batch_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seconds_since_last_batch: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ProbeFailure>,
}

impl CheckResult {
    pub fn probe(probe: ProbeResult) -> Self {
        Self {
            name: probe.name,
            status: probe.status,
            latency_ms: Some(probe.latency_ms),
            error: probe.error,
            transport: None,
            connection_state: None,
            thread_alive: None,
            last_batch_at: None,
            seconds_since_last_batch: None,
        }
    }

    pub fn ingest(status: Status, ingest: IngestHealth) -> Self {
        Self {
            name: "ingest".into(),
            status,
            latency_ms: None,
            error: None,
            transport: Some(ingest.transport),
            connection_state: Some(ingest.connection_state),
            thread_alive: Some(ingest.thread_alive),
            last_batch_at: ingest.last_batch_at,
            seconds_since_last_batch: ingest.seconds_since_last_batch,
        }
    }
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct LivenessResponse {
    pub status: Liveness,
    pub uptime_seconds: f64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ReadinessResponse {
    pub status: Readiness,
    pub checks: Vec<CheckResult>,
}

#[derive(Debug, Clone, Default, Serialize, ToSchema)]
pub struct ProcessStats {
    pub cpu_percent: f64,
    pub memory_rss_bytes: u64,
    pub num_threads: u64,
    pub open_files: u64,
}

#[derive(Debug, Clone, Default, Serialize, ToSchema)]
pub struct HostStats {
    pub cpu_percent: f64,
    pub memory_total_bytes: u64,
    pub memory_available_bytes: u64,
    pub memory_percent: f64,
}

#[derive(Debug, Clone, Default, Serialize, ToSchema)]
pub struct SystemSnapshot {
    pub process: ProcessStats,
    pub host: HostStats,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AppInfo {
    pub title: String,
    pub version: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct UptimeInfo {
    pub process_seconds: f64,
    /// Host boot time, unix seconds.
    pub system_boot_seconds: f64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct RequestInfo {
    pub last_request_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct DetailedStatusResponse {
    pub app: AppInfo,
    pub uptime: UptimeInfo,
    pub dependencies: Vec<ProbeResult>,
    pub ingest: IngestHealth,
    pub requests: RequestInfo,
    pub system: SystemSnapshot,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn statuses_serialise_as_the_contract_strings() {
        assert_eq!(serde_json::to_value(Status::Up).unwrap(), json!("up"));
        assert_eq!(serde_json::to_value(Status::Down).unwrap(), json!("down"));
        assert_eq!(
            serde_json::to_value(Readiness::Ready).unwrap(),
            json!("ready")
        );
        assert_eq!(
            serde_json::to_value(Readiness::NotReady).unwrap(),
            json!("not_ready")
        );
        assert_eq!(
            serde_json::to_value(Liveness::Alive).unwrap(),
            json!("alive")
        );
        assert_eq!(
            serde_json::to_value(ProbeFailure::Timeout).unwrap(),
            json!("timeout")
        );
        assert_eq!(
            serde_json::to_value(ConnectionState::Reconnecting).unwrap(),
            json!("reconnecting")
        );
        assert_eq!(
            serde_json::to_value(IngestTransport::Noop).unwrap(),
            json!("noop")
        );
    }
}
