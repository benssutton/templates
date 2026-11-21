"""OpenTelemetry tracing, off until an OTLP endpoint is configured.

Each app gets its own TracerProvider (never the global one), so isolated apps in
one process don't share exporters. An inbound W3C `traceparent` is continued.
"""

from fastapi import FastAPI
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    SimpleSpanProcessor,
    SpanExporter,
)

from settings import Settings


def configure_tracing(
    app: FastAPI, settings: Settings, exporter: SpanExporter | None = None
) -> TracerProvider | None:
    """Instrument `app` when an endpoint (or, in tests, an exporter) is given.
    Must run before the app serves its first request."""
    if exporter is None and settings.otel_exporter_otlp_endpoint is None:
        return None
    provider = TracerProvider(
        resource=Resource.create({"service.name": settings.otel_service_name})
    )
    if exporter is None:
        endpoint = settings.otel_exporter_otlp_endpoint
        provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{endpoint}/v1/traces"))
        )
    else:
        provider.add_span_processor(SimpleSpanProcessor(exporter))
    FastAPIInstrumentor.instrument_app(app, tracer_provider=provider)
    return provider
