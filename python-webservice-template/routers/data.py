import asyncio
import logging

import pyarrow as pa
from fastapi import APIRouter, HTTPException, Query, Request

from core.correlation import timed
from core.dependencies import DataServiceDep, StreamIngestServiceDep
from persistence.stream_store.lsm_store import InvalidBatchError
from schemas.data import DataRowsResponse, IngestAccepted
from services.stream_ingest import BatchTooLargeError

log = logging.getLogger(__name__)

TAG = "Data Service"
TAG_METADATA = {"name": TAG, "description": "Endpoints for retrieving data"}

router = APIRouter(tags=[TAG])


ARROW_MEDIA_TYPES = frozenset(
    {"application/vnd.apache.arrow.stream", "application/octet-stream"}
)


def _decode(body: bytes) -> list[pa.RecordBatch]:
    return list(pa.ipc.open_stream(pa.BufferReader(body)))


@router.get("", response_model=DataRowsResponse)
async def get_data(
    data_service: DataServiceDep,
    limit: int = Query(default=10, ge=1, le=100),
):
    return await data_service.get_data(limit=limit)


@router.get("/cache", response_model=DataRowsResponse)
async def get_cached_data(
    svc: StreamIngestServiceDep,
    limit: int = Query(default=10, ge=1, le=100),
):
    return await svc.get_data(limit=limit)


@router.post("/ingest", status_code=202, response_model=IngestAccepted)
async def ingest_batch(request: Request, svc: StreamIngestServiceDep):
    media_type = request.headers.get("content-type", "").split(";", 1)[0]
    if media_type.strip().lower() not in ARROW_MEDIA_TYPES:
        raise HTTPException(
            status_code=415,
            detail="Content-Type must be application/vnd.apache.arrow.stream",
        )
    body = await request.body()
    try:
        async with timed("ingest.decode"):
            # Decoding a 16 MiB stream is CPU work: keep it off the event loop.
            batches = await asyncio.to_thread(_decode, body)
    except (pa.ArrowInvalid, OSError) as exc:
        # The parser message describes internals; log it, answer generically.
        log.warning("rejecting unreadable Arrow IPC body: %s", exc)
        raise HTTPException(status_code=400, detail="Invalid Arrow IPC stream") from exc
    try:
        await svc.ingest_batches(batches)
    except BatchTooLargeError as exc:
        raise HTTPException(status_code=413, detail="Payload Too Large") from exc
    except InvalidBatchError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return IngestAccepted()
