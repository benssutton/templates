//! Health shapes; field names and types mirror python schemas/health.py.

use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;

/// MCP `get_health_status` (Python `HealthStatusResponse`).
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct HealthStatusResponse {
    pub status: String,
}

/// Result of a single dependency health probe.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ProbeResult {
    pub name: String,
    /// "up" | "down"
    pub status: String,
    pub latency_ms: f64,
    /// "unavailable" | "timeout" | null; never a raw error string.
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct IngestHealth {
    pub transport: String,
    /// "connected" | "reconnecting" | "down"
    pub connection_state: String,
    /// Name kept from Python (`thread_alive`): true while the ingest task runs.
    pub thread_alive: bool,
    pub last_batch_at: Option<DateTime<Utc>>,
    pub seconds_since_last_batch: Option<f64>,
    pub rows_ingested_total: u64,
    pub stale: bool,
}

/// One entry of the flat `/health/ready` checks array. `None` fields are
/// omitted (Python `response_model_exclude_none=True`).
#[derive(Debug, Clone, Default, Serialize, ToSchema)]
pub struct CheckResult {
    pub name: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transport: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_alive: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_batch_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seconds_since_last_batch: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct LivenessResponse {
    /// always "alive"
    pub status: String,
    pub uptime_seconds: f64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ReadinessResponse {
    /// "ready" | "not_ready"
    pub status: String,
    pub checks: Vec<CheckResult>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ProcessStats {
    pub cpu_percent: f64,
    pub memory_rss_bytes: u64,
    pub num_threads: u64,
    pub open_files: u64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct HostStats {
    pub cpu_percent: f64,
    pub memory_total_bytes: u64,
    pub memory_available_bytes: u64,
    pub memory_percent: f64,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
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
    /// Host boot time, unix seconds (Python `psutil.boot_time()`).
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
