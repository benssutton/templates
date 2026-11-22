"""The zstd module: in the standard library from Python 3.14, from the
backports-zstd package before that (same API, PEP 784)."""

import sys

if sys.version_info >= (3, 14):
    from compression import zstd
else:
    from backports import zstd  # pyright: ignore[reportMissingImports]

__all__ = ["zstd"]
