"""Content-coding negotiation (RFC 9110 section 12.5.3) for the two codings this
service offers. A missing Accept-Encoding header is treated as identity only:
the RFC allows any coding then, but probes and bare clients that send no header
cannot decode zstd."""

import math
from typing import Literal

Coding = Literal["zstd", "gzip"]


def _q(params: str) -> float:
    for param in params.split(";"):
        key, _, value = param.partition("=")
        if key.strip().lower() == "q":
            try:
                q = float(value.strip())
            except ValueError:
                return 0.0
            return min(max(q, 0.0), 1.0) if math.isfinite(q) else 0.0
    return 1.0


def _weights(accept_encoding: str) -> tuple[float, float]:
    """(q for zstd, q for gzip); an explicit entry beats `*`."""
    named: dict[str, float] = {}
    for item in accept_encoding.split(","):
        name, _, params = item.partition(";")
        name = name.strip().lower()
        if name:
            named[name] = _q(params)
    star = named.get("*", 0.0)
    return named.get("zstd", star), named.get("gzip", star)


def accepts_zstd(accept_encoding: str | None) -> bool:
    return accept_encoding is not None and _weights(accept_encoding)[0] > 0


def choose(accept_encoding: str | None) -> Coding | None:
    """zstd when acceptable and not out-weighted by gzip, else gzip, else None."""
    if accept_encoding is None:
        return None
    zstd, gzip = _weights(accept_encoding)
    if zstd > 0 and zstd >= gzip:
        return "zstd"
    if gzip > 0:
        return "gzip"
    return None
