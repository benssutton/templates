import threading
from collections.abc import Iterator

from ingestion.base import ConnectionState, Delivery, OnBound
from settings import Settings


class NoopBatchConsumer:
    """Connects to nothing and idles until closed. Selected with
    INGEST_TRANSPORT=noop for deployments (and tests) that only need the HTTP
    ingest path. Mirrors Java's NoopBatchConsumer."""

    def __init__(self, settings: Settings) -> None:
        self._closed = threading.Event()

    async def __aenter__(self) -> "NoopBatchConsumer":
        return self

    async def __aexit__(self, *_: object) -> None:
        self.close()

    def deliveries(self, on_bound: OnBound | None = None) -> Iterator[Delivery]:
        if on_bound is not None:
            on_bound()  # nothing to bind: always "bound"
        self._closed.wait()
        yield from ()

    def close(self) -> None:
        self._closed.set()

    def connection_state(self) -> ConnectionState:
        return (
            ConnectionState.DOWN if self._closed.is_set() else ConnectionState.CONNECTED
        )
