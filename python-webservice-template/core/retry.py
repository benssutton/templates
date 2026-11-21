import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable

log = logging.getLogger(__name__)


def jittered(delay: float) -> float:
    """delay plus up to 25% random jitter, so replicas do not retry in lockstep."""
    return delay + delay * 0.25 * random.random()


def backoff_delay(attempt: int, base_delay: float, max_delay: float) -> float:
    """base_delay * 2^(attempt-1), capped at max_delay, plus jitter."""
    return jittered(min(base_delay * 2 ** (attempt - 1), max_delay))


def _log_retry(
    label: str, attempt: int, max_attempts: int, delay: float, exc: Exception
) -> None:
    log.warning(
        "%s: attempt %d/%d failed, retrying in %.1fs: %s",
        label,
        attempt,
        max_attempts,
        delay,
        exc,
    )


async def connect_with_backoff[T](
    connect: Callable[[], Awaitable[T]],
    *,
    label: str,
    max_attempts: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
) -> T:
    """Call connect(), waiting base_delay * 2^(attempt-1) s (capped at max_delay)
    plus up to 25% jitter between attempts.

    After max_attempts failures the last exception propagates, aborting startup."""
    for attempt in range(1, max_attempts + 1):
        try:
            return await connect()
        except Exception as exc:
            if attempt == max_attempts:
                log.error("%s: all %d connection attempts failed", label, max_attempts)
                raise
            delay = backoff_delay(attempt, base_delay, max_delay)
            _log_retry(label, attempt, max_attempts, delay, exc)
            await asyncio.sleep(delay)
    # Only reachable when max_attempts < 1 (Settings enforces ge=1); makes the
    # 'always returns or raises' contract explicit to the type checker.
    raise ValueError(f"{label}: max_attempts must be >= 1, got {max_attempts}")


def _sleep(delay: float) -> bool:
    time.sleep(delay)
    return False


def connect_with_backoff_sync[T](
    connect: Callable[[], T],
    *,
    label: str,
    max_attempts: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    wait: Callable[[float], bool] = _sleep,
) -> T:
    """Blocking connect_with_backoff (same delays) for worker threads.

    wait(delay) sleeps and returns True to cancel (e.g. `threading.Event.wait`, so
    shutdown interrupts the backoff); a cancel re-raises the last attempt's error."""
    for attempt in range(1, max_attempts + 1):
        try:
            return connect()
        except Exception as exc:
            if attempt == max_attempts:
                log.error("%s: all %d connection attempts failed", label, max_attempts)
                raise
            delay = backoff_delay(attempt, base_delay, max_delay)
            _log_retry(label, attempt, max_attempts, delay, exc)
            if wait(delay):
                raise
    raise ValueError(f"{label}: max_attempts must be >= 1, got {max_attempts}")
