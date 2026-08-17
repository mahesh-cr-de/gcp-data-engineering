"""Cloud Logging-compatible structured logger setup."""

from __future__ import annotations

import json
import logging
import os
from typing import Any


class JsonFormatter(logging.Formatter):
    """Format records as one-line JSON for local and Cloud Logging use."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "severity": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        structured = getattr(record, "structured", None)
        if isinstance(structured, dict):
            payload.update(structured)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging() -> None:
    """Configure stdout JSON logging once per function instance."""
    root = logging.getLogger()
    root.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())
    if not any(getattr(handler, "_export_json", False) for handler in root.handlers):
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        handler._export_json = True  # type: ignore[attr-defined]
        root.handlers.clear()
        root.addHandler(handler)


def log_event(logger: logging.Logger, level: int, message: str, **fields: Any) -> None:
    """Emit a structured event without coupling domain code to a log backend."""
    logger.log(level, message, extra={"structured": fields})
