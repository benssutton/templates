import asyncio
import time
from collections.abc import Callable

import httpx
from httpx import AsyncClient, Response


async def eventually(
    predicate: Callable[[], bool],
    *,
    timeout: float = 30.0,
    interval: float = 0.2,
    what: str = "condition",
) -> None:
    """Poll a synchronous predicate until it returns True.

    The predicate runs in a worker thread, so a slow call (e.g. SEMP against a
    paused broker) can't freeze the app under test, which shares this loop.
    Transient transport errors count as "not yet"; any other exception
    propagates immediately."""
    deadline = time.monotonic() + timeout
    last_err: httpx.TransportError | None = None
    while time.monotonic() < deadline:
        try:
            if await asyncio.to_thread(predicate):
                return
        except httpx.TransportError as exc:
            last_err = exc
        await asyncio.sleep(interval)
    suffix = f"; last transport error: {last_err!r}" if last_err else ""
    raise AssertionError(f"{what} not met within {timeout}s{suffix}")


async def poll_json(
    client: AsyncClient,
    path: str,
    until: Callable[[Response, dict], bool],
    *,
    timeout: float,
    what: str,
) -> tuple[Response, dict]:
    """GET `path` until `until(resp, body)` holds; return that response and its
    JSON body. Non-JSON / non-object bodies count as "not yet". Raises an
    AssertionError with the last status and body on timeout."""
    deadline = time.monotonic() + timeout
    last = "no response"
    while time.monotonic() < deadline:
        resp = await client.get(path)
        last = f"status={resp.status_code} body={resp.text[:500]}"
        try:
            body = resp.json()
        except ValueError:
            body = None
        if isinstance(body, dict) and until(resp, body):
            return resp, body
        await asyncio.sleep(0.1)
    raise AssertionError(f"{what} not met within {timeout}s; last {last}")


async def poll_cache(
    client: AsyncClient,
    expected_total: int | None = None,
    *,
    until: Callable[[dict], bool] | None = None,
    timeout: float = 30.0,
) -> dict:
    """Poll /data/cache (2xx only) until it reports `expected_total` rows and/or
    `until(body)` holds. Use `until` to wait for an END state when an
    intermediate state can share the same total."""
    if expected_total is None and until is None:
        raise ValueError("poll_cache needs expected_total and/or until")

    def done(resp: Response, body: dict) -> bool:
        if not resp.is_success:
            return False
        if expected_total is not None and body.get("total") != expected_total:
            return False
        return until is None or until(body)

    _, body = await poll_json(
        client,
        "/data/cache?limit=100",
        done,
        timeout=timeout,
        what=f"cache state (total={expected_total}, until={until is not None})",
    )
    return body


async def poll_ready(
    client: AsyncClient,
    expected_code: int,
    *,
    until: Callable[[dict], bool] | None = None,
    timeout: float = 60.0,
) -> Response:
    """Poll /health/ready until it returns `expected_code` (and, if given,
    `until(body)` holds). The default timeout covers a Solace connect with
    backoff on a contended host."""
    resp, _ = await poll_json(
        client,
        "/health/ready",
        lambda r, b: r.status_code == expected_code and (until is None or until(b)),
        timeout=timeout,
        what=f"/health/ready == {expected_code}",
    )
    return resp


def check(body: dict, name: str) -> dict:
    """The named entry of a readiness body's `checks` list."""
    return {c["name"]: c for c in body["checks"]}[name]
