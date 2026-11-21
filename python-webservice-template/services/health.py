import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Mapping
from typing import Literal

import psutil

from core.last_request import LastRequest
from core.system_metrics import collect_system_snapshot
from schemas.health import (
    AppInfo,
    CheckResult,
    DetailedStatusResponse,
    IngestHealth,
    LivenessResponse,
    ProbeResult,
    ReadinessResponse,
    RequestInfo,
    UptimeInfo,
)
from services.stream_ingest import StreamIngestService
from settings import Settings

log = logging.getLogger(__name__)

# Returns when the dependency answers; raises when it does not.
Ping = Callable[[], Awaitable[None]]


class HealthService:
    def __init__(
        self,
        settings: Settings,
        pings: Mapping[str, Ping],
        ingest: StreamIngestService,
        last_request: LastRequest,
    ) -> None:
        self.settings = settings
        self._pings = dict(pings)
        self._ingest = ingest
        self._last_request = last_request
        self._started_at = time.monotonic()
        self._process = psutil.Process()
        self._process.cpu_percent()  # prime process CPU delta
        psutil.cpu_percent(interval=None)  # prime host CPU delta
        self._cached: tuple[float, list[ProbeResult]] | None = None
        self._probe_lock = asyncio.Lock()

    def _uptime_seconds(self) -> float:
        return time.monotonic() - self._started_at

    def liveness(self) -> LivenessResponse:
        return LivenessResponse(
            status="alive", uptime_seconds=round(self._uptime_seconds(), 3)
        )

    async def _probe(self, name: str, ping: Ping) -> ProbeResult:
        timeout = self.settings.health_check_timeout_seconds
        start = time.perf_counter()
        try:
            await asyncio.wait_for(ping(), timeout=timeout)
        except TimeoutError:
            return ProbeResult(
                name=name,
                status="down",
                latency_ms=round(timeout * 1000, 2),
                error="timeout",
            )
        except Exception as exc:
            # Full detail stays in the server log; the response carries a fixed token.
            log.error("%s health check failed: %s", name, exc)
            return ProbeResult(
                name=name,
                status="down",
                latency_ms=round((time.perf_counter() - start) * 1000, 2),
                error="unavailable",
            )
        return ProbeResult(
            name=name,
            status="up",
            latency_ms=round((time.perf_counter() - start) * 1000, 2),
        )

    async def dependencies(self) -> list[ProbeResult]:
        """Probe every dependency, reusing a result younger than
        health_cache_ttl_seconds; concurrent callers share one probe round."""
        ttl = self.settings.health_cache_ttl_seconds
        async with self._probe_lock:
            now = time.monotonic()
            if self._cached is not None and now - self._cached[0] < ttl:
                return self._cached[1]
            results = list(
                await asyncio.gather(
                    *(self._probe(name, ping) for name, ping in self._pings.items())
                )
            )
            self._cached = (time.monotonic(), results)
            return results

    async def _ingest_health(self) -> IngestHealth:
        return await self._ingest.health_check()

    def _ingest_status(self, ingest: IngestHealth) -> Literal["up", "down"]:
        if ingest.connection_state != "connected":
            return "down"
        if ingest.stale and self.settings.ingest_stale_fails_readiness:
            return "down"
        return "up"

    async def readiness(self) -> ReadinessResponse:
        deps = await self.dependencies()
        ingest = await self._ingest_health()
        ingest_status = self._ingest_status(ingest)

        checks = [
            CheckResult(
                name=d.name, status=d.status, latency_ms=d.latency_ms, error=d.error
            )
            for d in deps
        ]
        checks.append(
            CheckResult(
                name="ingest",
                status=ingest_status,
                transport=ingest.transport,
                connection_state=ingest.connection_state,
                thread_alive=ingest.thread_alive,
                last_batch_at=ingest.last_batch_at,
                seconds_since_last_batch=ingest.seconds_since_last_batch,
            )
        )

        all_up = all(d.status == "up" for d in deps) and ingest_status == "up"
        return ReadinessResponse(
            status="ready" if all_up else "not_ready", checks=checks
        )

    async def detailed_status(self) -> DetailedStatusResponse:
        deps = await self.dependencies()
        ingest = await self._ingest_health()
        snapshot = collect_system_snapshot(self._process)
        return DetailedStatusResponse(
            app=AppInfo(
                title=self.settings.app_title,
                version=self.settings.app_version,
                status=self.settings.status,
            ),
            uptime=UptimeInfo(
                process_seconds=round(self._uptime_seconds(), 3),
                system_boot_seconds=psutil.boot_time(),
            ),
            dependencies=deps,
            ingest=ingest,
            requests=RequestInfo(last_request_at=self._last_request.at),
            system=snapshot,
        )
