import math
from collections.abc import Iterator

from fastapi import FastAPI
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Info,
    PlatformCollector,
    ProcessCollector,
    generate_latest,
)
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, Metric
from prometheus_client.registry import Collector
from prometheus_fastapi_instrumentator import Instrumentator

from ingestion.base import ConnectionState
from schemas.health import DetailedStatusResponse
from services.stream_ingest import StreamIngestService
from settings import Settings


def _gauge(name: str, doc: str, value: float) -> GaugeMetricFamily:
    g = GaugeMetricFamily(name, doc)
    g.add_metric([], value)
    return g


class _AppCollector(Collector):
    """Every custom series, built at scrape time from the latest status snapshot
    and the ingest service's monotonic counts (true Prometheus counters without
    the service depending on prometheus_client)."""

    def __init__(self) -> None:
        self.ingest: StreamIngestService | None = None
        self.status: DetailedStatusResponse | None = None

    def collect(self) -> Iterator[Metric]:
        rows = CounterMetricFamily(
            "ingest_rows_ingested", "Rows written to the stream store"
        )
        outcomes = CounterMetricFamily(
            "ingest_outcomes",
            "Ingest messages (Solace) or requests (HTTP) by outcome",
            labels=["outcome"],
        )
        if self.ingest is not None:
            rows_total, counts = self.ingest.counts()
            rows.add_metric([], rows_total)
            for outcome, count in counts.items():
                outcomes.add_metric([outcome], count)
        yield rows
        yield outcomes
        if self.status is not None:
            yield from self._status_series(self.status)

    @staticmethod
    def _status_series(s: DetailedStatusResponse) -> Iterator[Metric]:
        up = GaugeMetricFamily(
            "dependency_up", "1 if dependency reachable else 0", labels=["name"]
        )
        latency = GaugeMetricFamily(
            "dependency_check_latency_seconds",
            "Dependency health-check latency",
            labels=["name"],
        )
        for dep in s.dependencies:
            up.add_metric([dep.name], 1.0 if dep.status == "up" else 0.0)
            latency.add_metric([dep.name], dep.latency_ms / 1000.0)
        yield up
        yield latency

        state = GaugeMetricFamily(
            "ingest_connection_state",
            "Ingest transport connection state (1=active)",
            labels=["transport", "state"],
        )
        for st in ConnectionState:
            state.add_metric(
                [s.ingest.transport, st.value],
                1.0 if s.ingest.connection_state == st.value else 0.0,
            )
        yield state
        secs = s.ingest.seconds_since_last_batch
        yield _gauge(
            "ingest_seconds_since_last_batch",
            "Seconds since last ingested batch",
            secs if secs is not None else math.nan,
        )

        p, h, up_s = s.system.process, s.system.host, s.uptime
        # app_ prefix: process_* belongs to prometheus_client's ProcessCollector.
        for name, doc, value in [
            ("app_process_cpu_percent", "Process CPU percent", p.cpu_percent),
            (
                "app_process_memory_rss_bytes",
                "Process resident memory bytes",
                p.memory_rss_bytes,
            ),
            ("app_process_num_threads", "Process thread count", p.num_threads),
            ("app_process_open_files", "Process open file count", p.open_files),
            (
                "app_process_uptime_seconds",
                "Process uptime seconds",
                up_s.process_seconds,
            ),
            ("system_cpu_percent", "Host CPU percent", h.cpu_percent),
            (
                "system_memory_total_bytes",
                "Host total memory bytes",
                h.memory_total_bytes,
            ),
            (
                "system_memory_available_bytes",
                "Host available memory bytes",
                h.memory_available_bytes,
            ),
            (
                "system_memory_used_percent",
                "Host memory used percent",
                h.memory_percent,
            ),
            (
                "system_boot_time_seconds",
                "Host boot time (unix seconds)",
                up_s.system_boot_seconds,
            ),
        ]:
            yield _gauge(name, doc, value)


class MetricsService:
    """Owns a per-app Prometheus registry (the multi-app test pattern forbids
    the global default registry) and builds the custom series on each scrape."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self.registry = CollectorRegistry()
        self._instrumentator = Instrumentator(registry=self.registry)
        # The standard process_* / python_* series (process ones on Linux only).
        ProcessCollector(registry=self.registry)
        PlatformCollector(registry=self.registry)
        self._collector = _AppCollector()
        self.registry.register(self._collector)

        self.app_info = Info("app", "Application info", registry=self.registry)
        self.app_info.info(
            {"title": settings.app_title, "version": settings.app_version}
        )

    def instrument(self, app: FastAPI) -> None:
        self._instrumentator.instrument(app)

    def bind_ingest(self, ingest: StreamIngestService) -> None:
        """Called by the lifespan once the ingest service exists."""
        self._collector.ingest = ingest

    def render(self, status: DetailedStatusResponse) -> tuple[bytes, str]:
        """Exposition text for this scrape's status snapshot."""
        self._collector.status = status
        return generate_latest(self.registry), CONTENT_TYPE_LATEST
