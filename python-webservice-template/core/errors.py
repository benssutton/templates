"""One error contract: every error response is application/json {"detail": ...}.

FastAPI already renders HTTPException (including routing 404/405) and request
validation errors that way. This module adds what it does not cover: unhandled
exceptions (500), store outages (503), and splitting FastAPI's catch-all 422 into
the contract's 400 (malformed JSON) and 415 (not JSON), plus a wrapper that gives
the mounted MCP app's own plain-text rejections the same shape.
"""

import json
import logging
from http import HTTPStatus

import asyncpg
import redis.exceptions
from clickhouse_connect.driver.exceptions import OperationalError
from fastapi import FastAPI, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send

log = logging.getLogger(__name__)

# Driver errors that mean "the store is unreachable or too slow", as opposed to
# a bug in the query. TimeoutError covers asyncpg command/acquire timeouts and
# OSError covers refused or reset connections.
STORE_UNAVAILABLE_ERRORS: tuple[type[Exception], ...] = (
    asyncpg.PostgresConnectionError,
    asyncpg.InterfaceError,
    redis.exceptions.ConnectionError,
    redis.exceptions.TimeoutError,
    OperationalError,
    TimeoutError,
    OSError,
)


async def _store_unavailable(request: Request, exc: Exception) -> JSONResponse:
    log.error("store unavailable on %s %s: %r", request.method, request.url.path, exc)
    return JSONResponse({"detail": "Service Unavailable"}, status_code=503)


async def _unhandled(request: Request, exc: Exception) -> JSONResponse:
    log.exception(
        "unhandled error on %s %s", request.method, request.url.path, exc_info=exc
    )
    return JSONResponse({"detail": "Internal Server Error"}, status_code=500)


def _is_json(content_type: str) -> bool:
    media_type = content_type.split(";", 1)[0].strip().lower()
    return media_type == "application/json" or media_type.endswith("+json")


async def _request_invalid(request: Request, exc: Exception) -> Response:
    if not isinstance(exc, RequestValidationError):  # registered for that type only
        raise exc
    errors = exc.errors()
    if any(e["type"] == "json_invalid" for e in errors):
        return JSONResponse({"detail": "Malformed JSON body"}, status_code=400)
    # With strict Content-Type checking FastAPI hands a JSON body route the raw
    # bytes when the header is missing or not JSON, which then fails validation.
    if any(e["loc"][:1] == ("body",) for e in errors) and not _is_json(
        request.headers.get("content-type", "")
    ):
        return JSONResponse(
            {"detail": "Content-Type must be application/json"}, status_code=415
        )
    return await request_validation_exception_handler(request, exc)


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(RequestValidationError, _request_invalid)
    for error_type in STORE_UNAVAILABLE_ERRORS:
        app.add_exception_handler(error_type, _store_unavailable)
    # Exception is special-cased by Starlette: it is served by the outermost
    # ServerErrorMiddleware, which still re-raises so the server logs it too.
    app.add_exception_handler(Exception, _unhandled)


class JsonErrorBodyMiddleware:
    """Rewrites a non-JSON error response into {"detail": <original text>}.

    For the mounted MCP app: its Host/Origin and size rejections are plain text.
    JSON error bodies (JSON-RPC errors) are part of the MCP protocol and pass
    through untouched. The SDK answers a foreign Host with 421; the contract
    is 403 for both Host and Origin.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        start: Message | None = None
        chunks: list[bytes] = []

        async def rewriting_send(message: Message) -> None:
            nonlocal start
            if message["type"] == "http.response.start":
                headers = dict(message.get("headers", []))
                content_type = headers.get(b"content-type", b"")
                if message["status"] >= 400 and b"json" not in content_type:
                    start = message  # hold it until the body is complete
                    return
            if start is None:
                await send(message)
                return
            chunks.append(message.get("body", b""))
            if message.get("more_body", False):
                return
            text = b"".join(chunks).decode("utf-8", "replace").strip()
            status = start["status"]
            detail = text or HTTPStatus(status).phrase
            if status == HTTPStatus.MISDIRECTED_REQUEST:
                status = HTTPStatus.FORBIDDEN
            body = json.dumps({"detail": detail}).encode()
            headers = [
                (k, v)
                for k, v in start.get("headers", [])
                if k not in (b"content-type", b"content-length")
            ]
            headers += [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
            ]
            await send(
                {"type": "http.response.start", "status": status, "headers": headers}
            )
            await send({"type": "http.response.body", "body": body})

        await self.app(scope, receive, rewriting_send)
