import asyncio
import threading
from typing import Iterator

import pyarrow as pa
import pyarrow.flight as flight

from ingestion.base import ConnectionState
from settings import Settings


class FlightBatchConsumer:
    """Async context manager for connection lifecycle; BatchConsumer for ingest."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: flight.FlightClient | None = None
        self._reader: flight.FlightStreamReader | None = None
        self._state = ConnectionState.DOWN
        self._closing = False
        # Serialises stream start-up (batches()) against close(). Without it a
        # reconnect can pass the _closing guard and then open a new reader that
        # close() has already finished tearing down, leaving the ingest thread
        # blocked on a reader nobody will cancel.
        self._lock = threading.Lock()

    async def __aenter__(self) -> "FlightBatchConsumer":
        location = f"grpc://{self._settings.flight_host}:{self._settings.flight_port}"
        self._client = await asyncio.to_thread(flight.connect, location)
        self._state = ConnectionState.RECONNECTING  # connected to server, not yet streaming
        return self

    async def __aexit__(self, *_: object) -> None:
        await asyncio.to_thread(self.close)

    def connection_state(self) -> ConnectionState:
        return self._state

    def batches(self) -> Iterator[pa.RecordBatch]:
        # Capture the client under the lock and re-check _closing, but run the
        # potentially-blocking do_get() OUTSIDE the lock so a concurrent close()
        # never has to wait on it. After do_get we re-acquire and publish the
        # reader only if close() hasn't fired meanwhile; if it has, we cancel the
        # orphan reader ourselves so it cannot leak.
        with self._lock:
            # close() may have run between reconnect attempts — exit cleanly so the
            # ingest loop terminates instead of dereferencing a closed client.
            if self._closing or self._client is None:
                return
            client = self._client
            self._state = ConnectionState.RECONNECTING
        ticket = flight.Ticket(self._settings.flight_ticket.encode())
        reader = client.do_get(ticket)            # raises if the server is unreachable
        with self._lock:
            if self._closing:                     # close() won the race during do_get
                self._cancel(reader)
                return
            self._reader = reader                 # tracked so close() can cancel it
            self._state = ConnectionState.CONNECTED
        try:
            for chunk in reader:
                yield chunk.data
        except Exception:
            if self._closing:
                return                              # intentional shutdown — exit cleanly
            self._state = ConnectionState.RECONNECTING
            raise
        finally:
            with self._lock:
                if self._reader is reader:          # don't clobber a reader close() swapped
                    self._reader = None
        if self._closing:
            return
        # Clean end of stream while not closing == an unexpected disconnect.
        self._state = ConnectionState.RECONNECTING
        raise ConnectionError("flight stream ended unexpectedly")

    @staticmethod
    def _cancel(reader: flight.FlightStreamReader) -> None:
        try:
            reader.cancel()
        except Exception:
            pass

    def close(self) -> None:
        # Take the reader/client references under the lock so a concurrently
        # starting batches() cannot publish a stream we then fail to cancel.
        with self._lock:
            self._closing = True
            self._state = ConnectionState.DOWN
            reader, self._reader = self._reader, None
            client, self._client = self._client, None
        # Cancel the in-flight do_get first: on Windows, client.close() alone does
        # not unblock a thread iterating the stream reader, so the ingest thread
        # would be abandoned while still holding the gRPC stream open (which in
        # turn blocks the server's shutdown). Cancelling the reader aborts that
        # read promptly so the ingest loop exits. Done outside the lock because
        # cancel()/close() can block briefly and must not stall batches() start-up.
        if reader is not None:
            self._cancel(reader)
        if client is not None:
            client.close()
