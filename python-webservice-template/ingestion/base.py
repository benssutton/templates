from dataclasses import dataclass
from enum import Enum
from typing import Callable, Iterator, Protocol

import pyarrow as pa


class ConnectionState(str, Enum):
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


class BatchConsumer(Protocol):
    """Synchronous interface run on the dedicated ingest thread.

    deliveries() is a blocking generator; close() must be thread-safe and
    unblock any pending deliveries() call so the ingest thread can exit.
    connection_state() is a cheap, cached read (no I/O).
    """

    def deliveries(self) -> Iterator[Delivery]: ...

    def close(self) -> None: ...

    def connection_state(self) -> ConnectionState: ...
