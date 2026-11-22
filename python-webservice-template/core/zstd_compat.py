"""The zstd module: in the standard library from Python 3.14, from the
backports-zstd package before that (same API, PEP 784)."""

try:
    from compression import zstd  # pyright: ignore[reportAttributeAccessIssue]
except ImportError:  # Python < 3.14
    from backports import zstd  # pyright: ignore[reportMissingImports]

__all__ = ["zstd"]
