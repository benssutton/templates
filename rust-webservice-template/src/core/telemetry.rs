//! Logging and tracing setup for the binary: structured JSON logs (or text
//! for local reading) filtered by `LOG_LEVEL`, and OpenTelemetry trace
//! export over OTLP/HTTP when `OTEL_EXPORTER_OTLP_ENDPOINT` is set. W3C
//! `traceparent` headers on inbound requests become the parent of the
//! request span (`set_remote_parent`), so a trace continues across services.
//! Tests install their own subscriber and never call `init`.

use crate::settings::{LogFormat, Settings};
use anyhow::Context as _;
use axum::http::HeaderMap;
use opentelemetry::propagation::Extractor;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

/// Flushes and stops the trace exporter when dropped.
pub struct Telemetry {
    provider: Option<SdkTracerProvider>,
}

impl Drop for Telemetry {
    fn drop(&mut self) {
        if let Some(provider) = self.provider.take() {
            if let Err(err) = provider.shutdown() {
                eprintln!("trace exporter shutdown failed: {err}");
            }
        }
    }
}

/// Install the global subscriber. Call once, from `main`.
pub fn init(settings: &Settings) -> anyhow::Result<Telemetry> {
    let filter = EnvFilter::try_new(&settings.log_level)
        .with_context(|| format!("invalid LOG_LEVEL {:?}", settings.log_level))?;
    let fmt = match settings.log_format {
        LogFormat::Json => tracing_subscriber::fmt::layer()
            .json()
            .with_current_span(true)
            .with_span_list(false)
            .boxed(),
        LogFormat::Text => tracing_subscriber::fmt::layer().boxed(),
    };
    let provider = match &settings.otel_exporter_otlp_endpoint {
        Some(endpoint) => Some(tracer_provider(endpoint, &settings.otel_service_name)?),
        None => None,
    };
    let otel = provider.as_ref().map(|p| {
        tracing_opentelemetry::layer().with_tracer(p.tracer(settings.otel_service_name.clone()))
    });
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt)
        .with(otel)
        .try_init()
        .context("installing the tracing subscriber")?;
    Ok(Telemetry { provider })
}

fn tracer_provider(endpoint: &str, service: &str) -> anyhow::Result<SdkTracerProvider> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(format!("{}/v1/traces", endpoint.trim_end_matches('/')))
        .build()
        .context("building the OTLP span exporter")?;
    Ok(SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            Resource::builder()
                .with_service_name(service.to_owned())
                .build(),
        )
        .build())
}

struct Headers<'a>(&'a HeaderMap);

impl Extractor for Headers<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

/// Make the remote caller's span (from `traceparent`) the parent of `span`.
/// A no-op when the request carries no trace context or export is off.
pub fn set_remote_parent(span: &tracing::Span, headers: &HeaderMap) {
    let parent = opentelemetry::global::get_text_map_propagator(|p| p.extract(&Headers(headers)));
    let _ = span.set_parent(parent);
}
