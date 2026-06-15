from typing import Annotated

from fastapi import Depends, Request

from core.container import Container
from services.cache import CacheService
from services.config import ConfigService
from services.data import DataService
from services.health import HealthService
from services.metrics import MetricsService
from services.stream_ingest import StreamIngestService


def get_container(request: Request) -> Container:
    # Resolved per-request from the owning app, so each app (including
    # isolated test apps in the same process) sees only its own services.
    return request.app.state.container


ContainerDep = Annotated[Container, Depends(get_container)]


def get_health_service(container: ContainerDep) -> HealthService:
    return container.get(HealthService)


def get_data_service(container: ContainerDep) -> DataService:
    return container.get(DataService)


def get_config_service(container: ContainerDep) -> ConfigService:
    return container.get(ConfigService)


def get_cache_service(container: ContainerDep) -> CacheService:
    return container.get(CacheService)


def get_stream_ingest_service(container: ContainerDep) -> StreamIngestService:
    return container.get(StreamIngestService)


def get_metrics_service(container: ContainerDep) -> MetricsService:
    return container.get(MetricsService)


HealthServiceDep = Annotated[HealthService, Depends(get_health_service)]
DataServiceDep = Annotated[DataService, Depends(get_data_service)]
ConfigServiceDep = Annotated[ConfigService, Depends(get_config_service)]
CacheServiceDep = Annotated[CacheService, Depends(get_cache_service)]
StreamIngestServiceDep = Annotated[StreamIngestService, Depends(get_stream_ingest_service)]
MetricsServiceDep = Annotated[MetricsService, Depends(get_metrics_service)]
