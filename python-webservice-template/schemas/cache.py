from typing import Any

from pydantic import BaseModel, Field, PositiveInt


class CacheSetRequest(BaseModel):
    key: str = Field(min_length=1)
    value: Any
    # Positive: Redis EXPIRE with 0 or a negative TTL deletes the key immediately.
    ttl_seconds: PositiveInt | None = None


class CacheEntry(BaseModel):
    key: str
    value: Any
    ttl_seconds: int | None = None
