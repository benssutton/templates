"""JSON logging, one object per line, with the correlation ID on every line.

uvicorn's loggers propagate here (main.py passes log_config=None), and its access
line is logged inside CorrelationIdMiddleware, so it carries the ID too. Only the
first call configures logging: tests build many apps per process, and a later
app must not reset a level a test has set.
"""

import json
import logging
from datetime import UTC, datetime

from core.correlation import CorrelationIdFilter

_HANDLER_NAME = "app-json"


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "correlation_id": getattr(record, "correlation_id", "-"),
        }
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def configure_logging(level: str = "INFO") -> None:
    root = logging.getLogger()
    if any(h.get_name() == _HANDLER_NAME for h in root.handlers):
        return
    root.setLevel(level.upper())
    handler = logging.StreamHandler()
    handler.set_name(_HANDLER_NAME)
    handler.setFormatter(JsonFormatter())
    handler.addFilter(CorrelationIdFilter())
    root.addHandler(handler)
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        uvicorn_logger = logging.getLogger(name)
        uvicorn_logger.handlers.clear()
        uvicorn_logger.propagate = True
