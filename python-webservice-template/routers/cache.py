from fastapi import APIRouter, HTTPException

from core.dependencies import CacheServiceDep
from schemas.cache import CacheEntry, CacheSetRequest

TAG = "Cache"
TAG_METADATA = {
    "name": TAG,
    "description": "Endpoints for reading and writing Redis cache entries",
}

router = APIRouter(tags=[TAG])


@router.post("", status_code=201)
async def set_cache(
    body: CacheSetRequest, cache_service: CacheServiceDep
) -> CacheEntry:
    return await cache_service.set(body.key, body.value, body.ttl_seconds)


# `:path`: a key containing `/` (sent as `%2F`) reaches this handler intact,
# instead of a 404 or a slash-redirect to a different key.
@router.get("/{key:path}")
async def get_cache(key: str, cache_service: CacheServiceDep) -> CacheEntry:
    entry = await cache_service.get(key)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found")
    return entry
