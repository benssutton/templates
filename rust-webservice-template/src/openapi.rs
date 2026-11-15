use crate::routers::{health, metrics, root};
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
    )),
    tags(
        (name = "API Root Page", description = "Service information"),
        (name = "Application Health", description = "Liveness, readiness and detailed status endpoints"),
        (name = "Observability", description = "Prometheus metrics"),
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
}
