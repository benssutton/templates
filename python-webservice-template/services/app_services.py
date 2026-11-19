from dataclasses import dataclass

from services.cache import CacheService
from services.config import ConfigService
from services.data import DataService
from services.health import HealthService
from services.metrics import MetricsService
from services.stream_ingest import StreamIngestService


@dataclass(frozen=True)
class AppServices:
    """Every service of one app, built by the lifespan once its stores are
    connected and stored on app.state.services. One instance per app, so
    isolated apps in one process (tests) never share services."""

    health: HealthService
    config: ConfigService
    cache: CacheService
    data: DataService
    stream_ingest: StreamIngestService
    metrics: MetricsService | None
