use crate::routers::{cache, config, data, health, metrics, root};
use crate::schemas::cache::{CacheEntry, CacheSetRequest};
use crate::schemas::config::{ConfigEntry, ConfigSetRequest};
use crate::schemas::data::{DataRowResponse, DataRowsResponse};
use crate::schemas::health::{
    AppInfo, CheckResult, DetailedStatusResponse, HostStats, IngestHealth, LivenessResponse,
    ProbeResult, ProcessStats, ReadinessResponse, RequestInfo, SystemSnapshot, UptimeInfo,
};
use crate::schemas::root::RootResponse;
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    paths(
        root::get_root,
        health::get_live,
        health::get_ready,
        health::get_status,
        metrics::get_metrics,
        config::get_config,
        config::set_config,
        data::get_data,
        cache::set_cache,
        cache::get_cache,
    ),
    components(schemas(
        RootResponse,
        LivenessResponse,
        ReadinessResponse,
        CheckResult,
        DetailedStatusResponse,
        AppInfo,
        UptimeInfo,
        ProbeResult,
        IngestHealth,
        RequestInfo,
        SystemSnapshot,
        ProcessStats,
        HostStats,
        ConfigSetRequest,
        ConfigEntry,
        DataRowResponse,
        DataRowsResponse,
        CacheSetRequest,
        CacheEntry,
    )),
    tags(
        (name = "API Root Page", description = "Service information"),
        (name = "Application Health", description = "Liveness, readiness and detailed status endpoints"),
        (name = "Observability", description = "Prometheus metrics"),
        (name = "Configuration", description = "Endpoints for managing key-value configuration settings"),
        (name = "Data Service", description = "Endpoints for retrieving data"),
        (name = "Cache", description = "Endpoints for reading and writing Redis cache entries"),
    ),
    info(
        title = "Rust Template Service",
        version = "1.0.0",
        description = "A Rust axum service with REST and MCP endpoints, ready for Claude",
    )
)]
pub struct ApiDoc;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_serialises_with_every_r1_path() {
        let json = ApiDoc::openapi().to_pretty_json().unwrap();
        for path in [
            "\"/\"",
            "/health/live",
            "/health/ready",
            "/health/status",
            "/metrics",
        ] {
            assert!(json.contains(path), "missing {path}");
        }
    }

    #[test]
    fn document_serialises_with_every_r2_path() {
        let json = ApiDoc::openapi().to_pretty_json().unwrap();
        for path in [
            "\"/config/\"",
            "\"/data\"",
            "\"/cache/\"",
            "\"/cache/{key}\"",
        ] {
            assert!(json.contains(path), "missing {path}");
        }
    }
}
