import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from typing import TypeVar

log = logging.getLogger(__name__)

T = TypeVar("T")


async def connect_with_backoff(
    connect: Callable[[], Awaitable[T]],
    *,
    label: str,
    max_attempts: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
) -> T:
    """Call connect() with randomised exponential backoff.

    On each failure, waits base_delay * 2^(attempt-1) seconds plus up to 25%
    random jitter. After max_attempts consecutive failures the final exception
    propagates, aborting the lifespan and exiting the process.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            return await connect()
        except Exception as exc:
            if attempt == max_attempts:
                log.error("%s: all %d connection attempts failed", label, max_attempts)
                raise
            delay = min(base_delay * 2 ** (attempt - 1), max_delay)
            jitter = delay * 0.25 * random.random()
            log.warning(
                "%s: attempt %d/%d failed – retrying in %.1fs: %s",
                label,
                attempt,
                max_attempts,
                delay + jitter,
                exc,
            )
            await asyncio.sleep(delay + jitter)
    # Only reachable when max_attempts < 1 (Settings enforces ge=1); makes the
    # 'always returns or raises' contract explicit to the type checker.
    raise ValueError(f"{label}: max_attempts must be >= 1, got {max_attempts}")
