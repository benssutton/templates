"""The {"detail": ...} error the content-coding layers answer themselves,
before the app (and the correlation-id, CORS and Server-Timing layers) run."""

import json
from collections.abc import Iterable

from starlette.types import Send


async def send_json_error(
    send: Send,
    status: int,
    detail: str,
    extra_headers: Iterable[tuple[bytes, bytes]] = (),
) -> None:
    body = json.dumps({"detail": detail}).encode()
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
                *extra_headers,
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})
