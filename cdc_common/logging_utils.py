"""Structured JSON logging with a stable key set and ``pipeline_run_id`` correlation."""

from __future__ import annotations

import json
import logging
import sys
from contextvars import ContextVar
from typing import Any

_RUN_ID: ContextVar[str | None] = ContextVar("pipeline_run_id", default=None)
_STD_ATTRS = set(vars(logging.LogRecord("", 0, "", 0, "", None, None))) | {"message"}


def set_run_id(run_id: str | None) -> None:
    """Attach ``pipeline_run_id`` to every log line emitted afterwards in this context."""
    _RUN_ID.set(run_id)


class JsonFormatter(logging.Formatter):
    """Render log records as one JSON object per line."""

    def format(self, record: logging.LogRecord) -> str:
        """Format ``record`` as JSON."""
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "severity": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "pipeline_run_id": _RUN_ID.get(),
        }
        payload.update({k: v for k, v in vars(record).items() if k not in _STD_ATTRS})
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Return a logger writing JSON lines to stdout (CloudWatch / Cloud Logging friendly)."""
    logger = logging.getLogger(name)
    if not any(isinstance(h.formatter, JsonFormatter) for h in logger.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.propagate = False
    logger.setLevel(level)
    return logger
