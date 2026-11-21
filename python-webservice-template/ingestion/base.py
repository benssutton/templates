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
    """One transport message: its decoded Arrow batches and settlement callbacks.

    StreamIngestService settles each delivery exactly once, AFTER the store write:
    accept() removes it; fail() redelivers it (to the DMQ after max-redelivery);
    reject() removes it for good (to the DMQ if configured). A raising callback is
    logged and never followed by a second settlement; the broker redelivers.

    CREATE_ON_START queues get the broker default of unlimited redelivery and no
    DMQ, so a persistently failing message retries forever (the ingest loop backs
    off between fail()s: jittered, doubling, capped). Production queues need
    max-redelivery and a DMQ.
    """

    batches: list[pa.RecordBatch]
    accept: Callable[[], None]
    fail: Callable[[], None]
    reject: Callable[[], None]
    redelivered: bool = False


OnBound = Callable[[], None]


class BatchConsumer(Protocol):
    """Synchronous transport interface, run on the dedicated ingest thread.

    deliveries() blocks; close() must be thread-safe and unblock it so the ingest
    thread can exit. deliveries()
    calls on_bound(), if given, once bound: proof of health, so the ingest loop
    resets its failure count even if no message arrives. connection_state() is a
    cached read (no I/O).
    """

    def deliveries(self, on_bound: OnBound | None = None) -> Iterator[Delivery]: ...

    def close(self) -> None: ...

    def connection_state(self) -> ConnectionState: ...
