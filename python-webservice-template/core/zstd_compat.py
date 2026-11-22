"""The zstd module: in the standard library from Python 3.14, from the
backports-zstd package before that (same API, PEP 784)."""

import sys

# A version gate rather than try/except, so the type checker analyses only the
# branch for its configured Python version.
if sys.version_info >= (3, 14):
    from compression import zstd
else:
    import backports.zstd as zstd  # pyright: ignore[reportMissingImports]

__all__ = ["zstd"]
