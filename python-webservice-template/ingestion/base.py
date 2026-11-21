from collections.abc import Callable, Iterator
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

import pyarrow as pa


class ConnectionState(StrEnum):
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    DOWN = "down"


@dataclass(frozen=True)
class Delivery:
    """One transport message: every Arrow batch decoded from it, plus the
    broker settlement callbacks. StreamIngestService settles each delivery
    exactly once, AFTER the store write:

    accept()  -> ACCEPTED: removed from the queue
    fail()    -> FAILED:   redelivered (after max-redelivery -> DMQ)
    reject()  -> REJECTED: removed, to the DMQ if configured; never redelivered

    A raising callback is logged by the ingest loop and never followed by a
    second settlement (the broker redelivers the unsettled message).

    Redelivery is bounded only by the queue: queues auto-created by the Solace
    consumer (CREATE_ON_START) get the broker default of unlimited
    max-redelivery and no DMQ, so a persistently failing message is retried
    forever. StreamIngestService backs off between consecutive fail()s
    (jittered, doubling, capped) to avoid a hot loop, but that is not a
    substitute for policy: for production queues an operator should configure
    max-redelivery and a dead-message queue.
    """

    batches: list[pa.RecordBatch]
    accept: Callable[[], None]
    fail: Callable[[], None]
    reject: Callable[[], None]
    redelivered: bool = False


OnBound = Callable[[], None]


class BatchConsumer(Protocol):
    """Synchronous interface run on the dedicated ingest thread.

    deliveries() is a blocking generator; close() must be thread-safe and
    unblock any pending deliveries() call so the ingest thread can exit.
    It calls on_bound() (if given) once the transport is bound and ready to
    deliver: proof the transport is healthy, so the ingest loop resets its
    consecutive-failure count even if no message ever arrives.
    connection_state() is a cheap, cached read (no I/O).
    """

    def deliveries(self, on_bound: OnBound | None = None) -> Iterator[Delivery]: ...

    def close(self) -> None: ...

    def connection_state(self) -> ConnectionState: ...
