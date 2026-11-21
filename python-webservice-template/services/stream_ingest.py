import asyncio
import contextlib
import logging
import os
import signal
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum

import pyarrow as pa

from core.boundary_timing import timed
from core.correlation import correlation_id_var, new_id, set_correlation_id
from core.retry import jittered
from ingestion.base import BatchConsumer, ConnectionState, Delivery
from persistence.stream_store.lsm_store import InvalidBatchError, LSMStore
from schemas.data import DataRowResponse, DataRowsResponse
from schemas.health import IngestHealth
from settings import Settings

log = logging.getLogger(__name__)


class BatchTooLargeError(ValueError):
    """A decoded batch exceeds max_ingest_batch_bytes. Like InvalidBatchError,
    retrying cannot help, so it is refused rather than redelivered."""


class _Outcome(Enum):
    # Values double as the `outcome` label of ingest_outcomes_total.
    ACCEPT = "accepted"
    FAIL = "failed"
    REJECT = "rejected"


def _default_shutdown() -> None:
    os.kill(os.getpid(), signal.SIGTERM)


@dataclass
class _Backoff:
    """Doubling, jittered wait from `base` to `cap`, with a failure count."""

    base: float
    cap: float
    current: float = field(init=False)
    failures: int = field(init=False, default=0)

    def __post_init__(self) -> None:
        self.current = self.base

    def reset(self) -> None:
        self.current, self.failures = self.base, 0

    def fail(self) -> float:
        """Count a failure; return the wait before the next attempt."""
        self.failures += 1
        delay = jittered(self.current)
        self.current = min(self.current * 2, self.cap)
        return delay


class StreamIngestService:
    def __init__(
        self,
        consumer: BatchConsumer,
        store: LSMStore,
        settings: Settings,
        shutdown_hook: Callable[[], None] | None = None,
    ) -> None:
        self._consumer = consumer
        self._store = store
        self._settings = settings
        self._request_shutdown = shutdown_hook or _default_shutdown
        self._thread: threading.Thread | None = None
        # Set first thing in __aexit__: wakes any backoff wait so close is prompt.
        self._stop = threading.Event()
        self._watchdog_task: asyncio.Task | None = None
        self._last_batch_at: datetime | None = None
        self._started_at: datetime = datetime.now(UTC)
        # Counters written by the ingest thread and HTTP worker threads alike.
        self._counts_lock = threading.Lock()
        self._rows_total = 0
        self._outcomes = {outcome.value: 0 for outcome in _Outcome}

    async def __aenter__(self) -> "StreamIngestService":
        self._thread = threading.Thread(target=self._ingest_loop, daemon=True)
        self._thread.start()
        if self._settings.ingest_max_disconnect_seconds is not None:
            self._watchdog_task = asyncio.create_task(self._disconnect_watchdog())
        return self

    async def __aexit__(self, *_: object) -> None:
        self._stop.set()
        if self._watchdog_task is not None:
            self._watchdog_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._watchdog_task
            self._watchdog_task = None
        # close() may block in the SDK (receiver terminate, disconnect): off-loop.
        await asyncio.to_thread(self._consumer.close)
        if self._thread is not None:
            join_timeout = self._settings.ingest_join_timeout_seconds
            await asyncio.to_thread(self._thread.join, join_timeout)
            if self._thread.is_alive():
                log.error(
                    "ingest thread did not stop within %.0fs; abandoning", join_timeout
                )
            self._thread = None

    def _check(self, batch: pa.RecordBatch) -> None:
        """Raise BatchTooLargeError or InvalidBatchError if the batch must be
        refused. Measures decoded Arrow nbytes (Java measures the encoded payload)."""
        max_bytes = self._settings.max_ingest_batch_bytes
        if batch.nbytes > max_bytes:
            log.error(
                "refusing ingest batch: %d bytes exceeds max_ingest_batch_bytes (%d)",
                batch.nbytes,
                max_bytes,
            )
            raise BatchTooLargeError(
                f"batch of {batch.nbytes} bytes exceeds the {max_bytes}-byte limit"
            )
        self._store.validate(batch)

    def _write(self, batch: pa.RecordBatch) -> None:
        self._store.ingest(batch)
        self._last_batch_at = datetime.now(UTC)
        with self._counts_lock:
            self._rows_total += batch.num_rows
        log.debug("ingested batch: rows=%d", batch.num_rows)

    def _write_all(self, batches: list[pa.RecordBatch]) -> None:
        """All-or-nothing for refusals: every batch is checked before any is
        written. An unexpected write error can still leave earlier batches
        written; a retry re-applies them, harmless under newest-wins merge."""
        for batch in batches:
            self._check(batch)
        for batch in batches:
            self._write(batch)

    def _decide(self, delivery: Delivery) -> _Outcome:
        """Write every batch and decide the outcome. Never settles: the caller
        settles exactly once, so a raising callback can't cause a second one."""
        try:
            self._write_all(delivery.batches)
        except BatchTooLargeError:
            return _Outcome.REJECT  # already logged by _check
        except InvalidBatchError:
            log.exception("rejecting unmergeable message")
            return _Outcome.REJECT
        except Exception:
            log.exception("ingest failed; message will be redelivered")
            return _Outcome.FAIL
        return _Outcome.ACCEPT

    def _settle(self, delivery: Delivery) -> _Outcome:
        """Settle once. A raising callback propagates to the ingest loop."""
        outcome = self._decide(delivery)
        self._count(outcome)
        if outcome is _Outcome.ACCEPT:
            delivery.accept()
        elif outcome is _Outcome.FAIL:
            delivery.fail()
        else:
            delivery.reject()
        return outcome

    def _ingest_loop(self) -> None:
        s = self._settings
        transport = _Backoff(s.ingest_retry_base_delay, s.ingest_retry_max_delay)
        # fail() makes the broker redeliver at once (CREATE_ON_START queues have
        # unlimited redelivery and no DMQ), so a persistent store error would spin
        # without its own backoff. Production queues should set max-redelivery + a DMQ.
        settle = _Backoff(s.ingest_retry_base_delay, s.ingest_retry_max_delay)
        while True:
            try:
                # A bind or a delivery proves the transport works.
                for delivery in self._consumer.deliveries(on_bound=transport.reset):
                    transport.reset()
                    outcome = self._settle_logged(delivery)
                    if outcome is _Outcome.FAIL:
                        if self._stop.wait(settle.fail()):
                            return
                    elif outcome is not None:
                        settle.reset()
                return  # the consumer was closed
            except Exception:
                delay = transport.fail()
                limit = s.ingest_max_consecutive_failures
                log.exception(
                    "consumer deliveries() failed (failure %d/%d)",
                    transport.failures,
                    limit,
                )
                if limit and transport.failures >= limit:
                    log.critical(
                        "ingest: %d consecutive failures; requesting shutdown",
                        transport.failures,
                    )
                    self._request_shutdown()
                    return
                if self._stop.wait(delay):
                    return

    def _settle_logged(self, delivery: Delivery) -> _Outcome | None:
        """Settle under a fresh correlation id. None when settling itself failed:
        the broker still holds the message and redelivers it."""
        token = set_correlation_id(new_id())
        try:
            return self._settle(delivery)
        except Exception:
            if self._stop.is_set():
                log.warning("settlement failed during shutdown; broker will redeliver")
            else:
                log.exception("settlement failed; broker will redeliver")
            return None
        finally:
            correlation_id_var.reset(token)

    async def _disconnect_watchdog(self) -> None:
        threshold = self._settings.ingest_max_disconnect_seconds
        if threshold is None:  # watchdog is only started when a threshold is set
            return
        poll = min(threshold / 2, 5.0)
        disconnected_since: float | None = None
        try:
            while True:
                await asyncio.sleep(poll)
                if self._consumer.connection_state() == ConnectionState.CONNECTED:
                    disconnected_since = None
                    continue
                now = asyncio.get_running_loop().time()
                if disconnected_since is None:
                    disconnected_since = now
                elif now - disconnected_since >= threshold:
                    log.critical(
                        "ingest transport not connected for %.0fs; requesting shutdown",
                        threshold,
                    )
                    self._request_shutdown()
                    return
        except asyncio.CancelledError:
            return

    def health_check(self) -> IngestHealth:
        state = self._consumer.connection_state()
        last = self._last_batch_at
        now = datetime.now(UTC)
        seconds_since = (now - last).total_seconds() if last is not None else None
        threshold = self._settings.ingest_staleness_threshold_seconds
        # Stale when explicitly configured and either:
        # - a batch was received but too long ago, or
        # - no batch has ever been received (use time-since-start as proxy).
        if threshold is not None:
            elapsed = (
                seconds_since
                if seconds_since is not None
                else (now - self._started_at).total_seconds()
            )
            stale = elapsed > threshold
        else:
            stale = False
        return IngestHealth(
            transport=self._settings.ingest_transport,
            connection_state=state.value,
            thread_alive=self._thread.is_alive() if self._thread is not None else False,
            last_batch_at=last,
            seconds_since_last_batch=(
                round(seconds_since, 3) if seconds_since is not None else None
            ),
            rows_ingested_total=self._rows_total,
            stale=stale,
        )

    async def get_data(self, limit: int) -> DataRowsResponse:
        async with timed("lsm.query"):
            rows, total = await asyncio.to_thread(self._store.query, limit)
        return DataRowsResponse(
            rows=[DataRowResponse(**r) for r in rows], total=total, limit=limit
        )

    async def ingest_batches(self, batches: list[pa.RecordBatch]) -> None:
        """Check every batch, then write them all (see _write_all). Raises
        BatchTooLargeError or InvalidBatchError with nothing written."""
        try:
            async with timed("ingest.lsm_write"):
                await asyncio.to_thread(self._write_all, batches)
        except (BatchTooLargeError, InvalidBatchError):
            self._count(_Outcome.REJECT)
            raise
        except Exception:
            self._count(_Outcome.FAIL)
            raise
        self._count(_Outcome.ACCEPT)

    def _count(self, outcome: _Outcome) -> None:
        with self._counts_lock:
            self._outcomes[outcome.value] += 1

    def counts(self) -> tuple[int, dict[str, int]]:
        """(rows ingested, outcome -> count) since start, for the metrics scrape."""
        with self._counts_lock:
            return self._rows_total, dict(self._outcomes)
