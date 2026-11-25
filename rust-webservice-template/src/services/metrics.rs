//! Per-app Prometheus metrics. The recorder is owned by the app and used via
//! `metrics::with_local_recorder`, never installed globally, so isolated
//! apps in one test process cannot collide.

use crate::ingestion::ConnectionState;
use crate::schemas::health::{DetailedStatusResponse, Status};
use crate::services::health::HealthService;
use crate::settings::Settings;
use axum::{
    extract::{MatchedPath, Request, State},
    middleware::Next,
    response::Response,
};
use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::{
    Matcher, PrometheusBuilder, PrometheusHandle, PrometheusRecorder,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Buckets (seconds) for `http_request_duration_seconds`, per route: fine
/// below 100 ms, where a healthy API answers, then coarse up to the request
/// timeout. `+Inf` is implicit.
pub const HTTP_DURATION_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// Buckets (bytes) for the request and response size histograms.
pub const HTTP_SIZE_BUCKETS: &[f64] = &[
    100.0,
    1_000.0,
    10_000.0,
    100_000.0,
    1_000_000.0,
    10_000_000.0,
    100_000_000.0,
];

/// Buckets (seconds) for the label-free `http_request_duration_highr_seconds`.
pub const HTTP_DURATION_HIGHR_BUCKETS: &[f64] = &[
    0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0,
    7.5, 10.0, 30.0, 60.0,
];

pub struct MetricsService {
    recorder: PrometheusRecorder,
    handle: PrometheusHandle,
}

impl MetricsService {
    pub fn new(settings: &Settings) -> anyhow::Result<Self> {
        let recorder = PrometheusBuilder::new()
            .set_buckets_for_metric(
                Matcher::Full("http_request_duration_seconds".to_owned()),
                HTTP_DURATION_BUCKETS,
            )
            .map_err(|e| anyhow::anyhow!("invalid prometheus buckets: {e}"))?
            .set_buckets_for_metric(
                Matcher::Full("http_request_duration_highr_seconds".to_owned()),
                HTTP_DURATION_HIGHR_BUCKETS,
            )
            .map_err(|e| anyhow::anyhow!("invalid prometheus buckets: {e}"))?
            .set_buckets_for_metric(Matcher::Suffix("_size_bytes".to_owned()), HTTP_SIZE_BUCKETS)
            .map_err(|e| anyhow::anyhow!("invalid prometheus buckets: {e}"))?
            .build_recorder();
        let handle = recorder.handle();
        let service = Self { recorder, handle };
        let (title, version) = (settings.app_title.clone(), settings.app_version.clone());
        service.with(|| {
            gauge!("app_info", "title" => title, "version" => version).set(1.0);
            // Exported at 0 from startup, so rate() and absent-series alerts
            // work before the first delivery.
            for outcome in ["accepted", "failed", "rejected"] {
                counter!("ingest_outcomes_total", "outcome" => outcome).absolute(0);
            }
        });
        Ok(service)
    }

    fn with<T>(&self, f: impl FnOnce() -> T) -> T {
        metrics::with_local_recorder(&self.recorder, f)
    }

    pub fn record_request(&self, method: &str, handler: &str, status: u16, elapsed: Duration) {
        let (method, handler) = (method.to_owned(), handler.to_owned());
        self.with(|| {
            counter!(
                "http_requests_total",
                "method" => method.clone(),
                "handler" => handler.clone(),
                "status" => status_class(status)
            )
            .increment(1);
            histogram!("http_request_duration_seconds", "method" => method, "handler" => handler)
                .record(elapsed.as_secs_f64());
            histogram!("http_request_duration_highr_seconds").record(elapsed.as_secs_f64());
        });
    }

    /// One settled delivery or HTTP ingest request. `outcome`: "accepted" |
    /// "failed" | "rejected".
    pub fn record_outcome(&self, outcome: &'static str) {
        self.with(|| counter!("ingest_outcomes_total", "outcome" => outcome).increment(1));
    }

    /// Rows written to the LSM store, by either ingest path.
    pub fn record_ingested_rows(&self, rows: u64) {
        self.with(|| counter!("ingest_rows_ingested_total").increment(rows));
    }

    /// Live rows the LSM memory bound evicted.
    pub fn record_evicted_rows(&self, rows: u64) {
        if rows > 0 {
            self.with(|| counter!("lsm_evicted_rows_total").increment(rows));
        }
    }

    pub fn record_sizes(&self, handler: &str, request_bytes: u64, response_bytes: u64) {
        let handler = handler.to_owned();
        self.with(|| {
            histogram!("http_request_size_bytes", "handler" => handler.clone())
                .record(request_bytes as f64);
            histogram!("http_response_size_bytes", "handler" => handler)
                .record(response_bytes as f64);
        });
    }

    /// Refresh the scrape-time gauges from a detailed status snapshot.
    pub fn refresh(&self, status: &DetailedStatusResponse) {
        self.with(|| {
            for dep in &status.dependencies {
                let up = if dep.status == Status::Up { 1.0 } else { 0.0 };
                gauge!("dependency_up", "name" => dep.name.clone()).set(up);
                gauge!("dependency_check_latency_seconds", "name" => dep.name.clone())
                    .set(dep.latency_ms / 1000.0);
            }
            let ingest = &status.ingest;
            for state in ConnectionState::ALL {
                let active = if ingest.connection_state == state {
                    1.0
                } else {
                    0.0
                };
                gauge!(
                    "ingest_connection_state",
                    "transport" => ingest.transport.as_str(),
                    "state" => state.as_str()
                )
                .set(active);
            }
            gauge!("ingest_seconds_since_last_batch")
                .set(ingest.seconds_since_last_batch.unwrap_or(f64::NAN));
            gauge!("lsm_rows").set(ingest.store.rows as f64);
            gauge!("lsm_parts").set(ingest.store.parts as f64);
            counter!("lsm_merge_failures_total").absolute(ingest.store.merge_failures_total);
            gauge!("lsm_poisoned").set(f64::from(u8::from(ingest.store.poisoned)));
            gauge!("lsm_max_rows").set(ingest.store.max_rows as f64);

            let (process, host) = (&status.system.process, &status.system.host);
            gauge!("app_process_cpu_percent").set(process.cpu_percent);
            gauge!("app_process_memory_rss_bytes").set(process.memory_rss_bytes as f64);
            gauge!("app_process_num_threads").set(process.num_threads as f64);
            gauge!("app_process_open_files").set(process.open_files as f64);
            gauge!("app_process_uptime_seconds").set(status.uptime.process_seconds);
            gauge!("system_cpu_percent").set(host.cpu_percent);
            gauge!("system_memory_total_bytes").set(host.memory_total_bytes as f64);
            gauge!("system_memory_available_bytes").set(host.memory_available_bytes as f64);
            gauge!("system_memory_used_percent").set(host.memory_percent);
            gauge!("system_boot_time_seconds").set(status.uptime.system_boot_seconds);
        });
    }

    /// Refresh from the health service, then render the exposition text.
    pub async fn render(&self, health: &HealthService) -> String {
        let status = health.detailed_status().await;
        self.refresh(&status);
        self.handle.run_upkeep();
        self.handle.render()
    }
}

fn status_class(status: u16) -> &'static str {
    match status / 100 {
        1 => "1xx",
        2 => "2xx",
        3 => "3xx",
        4 => "4xx",
        _ => "5xx",
    }
}

/// Axum middleware recording, per matched route, `http_requests_total`,
/// `http_request_duration_seconds`, `http_request_duration_highr_seconds` and
/// the request/response size series.
///
/// Attach it with `Router::layer` (after routing) so `MatchedPath` is present;
/// otherwise every request is recorded under `handler="none"`.
pub async fn middleware(
    State(metrics): State<Arc<MetricsService>>,
    req: Request,
    next: Next,
) -> Response {
    let method = req.method().as_str().to_owned();
    let handler = req
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str().to_owned())
        .unwrap_or_else(|| "none".to_owned());
    let request_bytes = content_length(req.headers()).unwrap_or(0);
    let started = Instant::now();
    let response = next.run(req).await;
    metrics.record_request(
        &method,
        &handler,
        response.status().as_u16(),
        started.elapsed(),
    );
    // hyper adds Content-Length only after this layer, so fall back to the
    // body's exact size hint (0 for a streamed body).
    let response_bytes = content_length(response.headers())
        .or_else(|| axum::body::HttpBody::size_hint(response.body()).exact())
        .unwrap_or(0);
    metrics.record_sizes(&handler, request_bytes, response_bytes);
    response
}

fn content_length(headers: &axum::http::HeaderMap) -> Option<u64> {
    headers
        .get(axum::http::header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .parse()
        .ok()
}
