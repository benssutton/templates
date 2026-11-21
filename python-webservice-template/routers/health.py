from fastapi import APIRouter, Response

from core.dependencies import HealthServiceDep
from schemas.health import DetailedStatusResponse, LivenessResponse, ReadinessResponse

TAG = "Application Health"
TAG_METADATA = {
    "name": TAG,
    "description": "Liveness, readiness and detailed status endpoints",
}

router = APIRouter(tags=[TAG])


@router.get("/live")
async def get_live(health_service: HealthServiceDep) -> LivenessResponse:
    return health_service.liveness()


@router.get("/ready", response_model_exclude_none=True)
async def get_ready(
    health_service: HealthServiceDep, response: Response
) -> ReadinessResponse:
    result = await health_service.readiness()
    if result.status != "ready":
        response.status_code = 503
    return result


@router.get("/status")
async def get_status(health_service: HealthServiceDep) -> DetailedStatusResponse:
    return await health_service.detailed_status()
