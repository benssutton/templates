"""Idempotent logging configuration that surfaces the correlation ID.

Installs a stream handler on the root logger whose formatter includes
[%(correlation_id)s], and attaches CorrelationIdFilter so the attribute always
exists. Safe to call multiple times — the multi-app test pattern constructs many
apps in one process.
"""
import logging

from core.correlation import CorrelationIdFilter

_CONFIGURED = False
_FORMAT = "%(asctime)s %(levelname)s [%(correlation_id)s] %(name)s: %(message)s"


def configure_logging(level: int = logging.INFO) -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(_FORMAT))
    handler.addFilter(CorrelationIdFilter())
    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(level)
    _CONFIGURED = True
