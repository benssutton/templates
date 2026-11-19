"""Typed FastAPI dependencies resolving this app's services from app.state."""

from typing import Annotated

from fastapi import Depends, HTTPException, Request

from services.app_services import AppServices
from services.cache import CacheService
from services.config import ConfigService
from services.data import DataService
from services.health import HealthService
from services.metrics import MetricsService
from services.stream_ingest import StreamIngestService


def get_services(request: Request) -> AppServices:
    # Resolved per request from the owning app, so each isolated app (including
    # test apps in the same process) sees only its own services.
    return request.app.state.services


ServicesDep = Annotated[AppServices, Depends(get_services)]


def get_health_service(services: ServicesDep) -> HealthService:
    return services.health


def get_data_service(services: ServicesDep) -> DataService:
    return services.data


def get_config_service(services: ServicesDep) -> ConfigService:
    return services.config


def get_cache_service(services: ServicesDep) -> CacheService:
    return services.cache


def get_stream_ingest_service(services: ServicesDep) -> StreamIngestService:
    return services.stream_ingest


def get_metrics_service(services: ServicesDep) -> MetricsService:
    if services.metrics is None:  # the /metrics route is only mounted when enabled
        raise HTTPException(status_code=404, detail="Not Found")
    return services.metrics


HealthServiceDep = Annotated[HealthService, Depends(get_health_service)]
DataServiceDep = Annotated[DataService, Depends(get_data_service)]
ConfigServiceDep = Annotated[ConfigService, Depends(get_config_service)]
CacheServiceDep = Annotated[CacheService, Depends(get_cache_service)]
StreamIngestServiceDep = Annotated[
    StreamIngestService, Depends(get_stream_ingest_service)
]
MetricsServiceDep = Annotated[MetricsService, Depends(get_metrics_service)]
