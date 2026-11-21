from fastapi import APIRouter, Response

from core.dependencies import HealthServiceDep, MetricsServiceDep

router = APIRouter()


@router.get("/metrics")
async def get_metrics(
    health_service: HealthServiceDep, metrics_service: MetricsServiceDep
) -> Response:
    body, content_type = metrics_service.render(await health_service.detailed_status())
    return Response(content=body, media_type=content_type)
