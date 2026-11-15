import asyncio
import logging
import os
import random
import signal
import threading
from datetime import datetime, timezone
from enum import Enum
from typing import Callable

import pyarrow as pa

from core.correlation import correlation_id_var, new_id, set_correlation_id, timed
from ingestion.base import BatchConsumer, ConnectionState, Delivery
from persistence.stream_store.lsm_store import InvalidBatchError, LSMStore
from schemas.data import DataRowResponse, DataRowsResponse
from schemas.health import IngestHealth
from settings import Settings

log = logging.getLogger(__name__)

_INGEST_BASE_DELAY = 1.0  # seconds before first retry
_INGEST_MAX_DELAY = 60.0  # seconds — retry cap
_INGEST_MAX_FAILURES = 5  # consecutive deliveries() failures before shutdown
_JOIN_TIMEOUT = 10.0  # seconds to wait for ingest thread on shutdown


class _Outcome(Enum):
    ACCEPT = "accept"
    FAIL = "fail"
    REJECT = "reject"


def _default_shutdown() -> None:
    os.kill(os.getpid(), signal.SIGTERM)


def _jittered(delay: float) -> float:
    return delay + delay * 0.25 * random.random()


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
        self._started_at: datetime = datetime.now(timezone.utc)
        self._rows_total = 0

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
            try:
                await self._watchdog_task
            except asyncio.CancelledError:
                pass
            self._watchdog_task = None
        self._consumer.close()
        if self._thread is not None:
            await asyncio.to_thread(self._thread.join, _JOIN_TIMEOUT)
            if self._thread.is_alive():
                log.error(
                    "ingest thread did not stop within %.0fs; abandoning", _JOIN_TIMEOUT
                )
            self._thread = None

    def _record_ingest(self, batch: pa.RecordBatch) -> bool:
        """Write one batch. False = refused as oversized (logged). Raises
        InvalidBatchError for an unmergeable batch; anything else is unexpected."""
        max_bytes = self._settings.max_ingest_batch_bytes
        if batch.nbytes > max_bytes:
            log.error(
                "dropping ingest batch: %d bytes exceeds max_ingest_batch_bytes (%d)",
                batch.nbytes,
                max_bytes,
            )
            return False
        self._store.ingest(batch)
        self._last_batch_at = datetime.now(timezone.utc)
        self._rows_total += batch.num_rows
        log.debug("ingested batch: rows=%d", batch.num_rows)
        return True

    def _decide(self, delivery: Delivery) -> _Outcome:
        """Write every batch and decide the outcome. Never settles: the caller
        settles exactly once, so a raising callback can't cause a second one."""
        # One message may carry several batches; settle once, after all of them.
        # A FAILED message is redelivered whole, so batches written before the
        # failure are re-applied - harmless under newest-wins merge semantics.
        try:
            for batch in delivery.batches:
                if not self._record_ingest(batch):
                    return _Outcome.REJECT
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
        if outcome is _Outcome.ACCEPT:
            delivery.accept()
        elif outcome is _Outcome.FAIL:
            delivery.fail()
        else:
            delivery.reject()
        return outcome

    def _ingest_loop(self) -> None:
        consecutive_failures = 0
        delay = _INGEST_BASE_DELAY
        # Settlement backoff, separate from the transport backoff (`delay`): fail()
        # makes the broker redeliver immediately (CREATE_ON_START queues have
        # unlimited max-redelivery and no DMQ), so a persistent store error
        # would otherwise spin. This only prevents a hot loop; production
        # queues should set max-redelivery + a DMQ (see Delivery docstring).
        settle_delay = _INGEST_BASE_DELAY
        # ingest_max_disconnect_seconds=None disables *all* automatic shutdown:
        # the watchdog (see __aenter__) and this consecutive-failure trigger.
        shutdown_on_failure = self._settings.ingest_max_disconnect_seconds is not None
        while True:
            try:
                for delivery in self._consumer.deliveries():
                    # Receiving a delivery proves the transport works.
                    consecutive_failures = 0
                    delay = _INGEST_BASE_DELAY
                    outcome: _Outcome | None = None
                    token = set_correlation_id(
                        new_id()
                    )  # per-message ID for this thread's logs
                    try:
                        outcome = self._settle(delivery)
                    except Exception:
                        # settle() calls into the SDK; if the session just dropped,
                        # the broker still holds the unsettled message and will
                        # redeliver it.
                        if self._stop.is_set():  # expected while shutting down
                            log.warning(
                                "settlement failed during shutdown; broker will redeliver"
                            )
                        else:
                            log.exception("settlement failed; broker will redeliver")
                    finally:
                        # Reset so backoff/disconnect logs on the error path below
                        # are not mis-attributed to the last message's ID.
                        correlation_id_var.reset(token)
                    if outcome is _Outcome.FAIL:
                        if self._stop.wait(_jittered(settle_delay)):
                            return  # shutting down
                        settle_delay = min(settle_delay * 2, _INGEST_MAX_DELAY)
                    elif outcome is not None:
                        settle_delay = _INGEST_BASE_DELAY
                return  # deliveries() returned cleanly - consumer was closed
            except Exception:
                consecutive_failures += 1
                log.exception(
                    "consumer deliveries() failed (failure %d/%d)",
                    consecutive_failures,
                    _INGEST_MAX_FAILURES,
                )
                if shutdown_on_failure and consecutive_failures >= _INGEST_MAX_FAILURES:
                    log.critical(
                        "ingest: %d consecutive failures; requesting shutdown",
                        consecutive_failures,
                    )
                    self._request_shutdown()
                    return
                if self._stop.wait(_jittered(delay)):
                    return  # shutting down
                delay = min(delay * 2, _INGEST_MAX_DELAY)

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

    async def health_check(self) -> IngestHealth:
        state = self._consumer.connection_state()
        last = self._last_batch_at
        now = datetime.now(timezone.utc)
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

    async def ingest_batch(self, batch: pa.RecordBatch) -> None:
        async with timed("ingest.lsm_write"):
            await asyncio.to_thread(self._record_ingest, batch)
