"""zstd/gzip content coding (RFC 9110) in three pure-ASGI layers, outermost
first, each with one job:

1. ContentNegotiationMiddleware (negotiation.py): strict mode's 406, 415 for
   a request Content-Encoding other than zstd or gzip, and the request's
   Accept-Encoding rewritten to the one coding the response will use.
2. ResponseCompressionMiddleware (response.py): compresses text-based
   responses with that coding and adds Vary: Accept-Encoding.
3. RequestDecompressionMiddleware (request.py): decodes zstd/gzip request
   bodies; MaxBodySizeMiddleware sits inside it, so the body limit counts
   decompressed bytes.

The 406 and 415 answered here sit outside the correlation-id, CORS and
Server-Timing layers, so they carry none of those headers.
"""

from starlette.applications import Starlette

from core.compression.negotiation import ContentNegotiationMiddleware
from core.compression.request import RequestDecompressionMiddleware
from core.compression.response import ResponseCompressionMiddleware

__all__ = [
    "ContentNegotiationMiddleware",
    "RequestDecompressionMiddleware",
    "ResponseCompressionMiddleware",
    "add_content_coding",
]


def add_content_coding(
    app: Starlette,
    *,
    max_body_bytes: int,
    enabled: bool = True,
    min_bytes: int = 1024,
    zstd_level: int = 3,
    require_zstd: bool = False,
) -> None:
    """Install the three layers. Starlette makes the last middleware added the
    outermost, so they are added innermost first. Add MaxBodySizeMiddleware
    before calling this: it must sit inside request decompression."""
    app.add_middleware(RequestDecompressionMiddleware, max_body_bytes=max_body_bytes)
    if enabled:
        # Strict mode compresses every eligible response, however small.
        app.add_middleware(
            ResponseCompressionMiddleware,
            min_bytes=0 if require_zstd else min_bytes,
            zstd_level=zstd_level,
        )
    app.add_middleware(
        ContentNegotiationMiddleware, enabled=enabled, require_zstd=require_zstd
    )
