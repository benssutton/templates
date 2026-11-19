from fastapi import APIRouter, HTTPException

from core.dependencies import CacheServiceDep
from schemas.cache import CacheEntry, CacheSetRequest

TAG = "Cache"
TAG_METADATA = {
    "name": TAG,
    "description": "Endpoints for reading and writing Redis cache entries",
}

router = APIRouter(tags=[TAG])


@router.post("", response_model=CacheEntry, status_code=201)
@router.post("/", response_model=CacheEntry, status_code=201, include_in_schema=False)
async def set_cache(body: CacheSetRequest, cache_service: CacheServiceDep):
    return await cache_service.set(body.key, body.value, body.ttl_seconds)


@router.get("/{key}", response_model=CacheEntry)
async def get_cache(key: str, cache_service: CacheServiceDep):
    entry = await cache_service.get(key)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found")
    return entry
