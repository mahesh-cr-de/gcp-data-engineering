"""Google Cloud Functions Gen2 HTTP entry point."""

from __future__ import annotations

import logging
from typing import Any

import functions_framework
from flask import Request, jsonify

from exceptions import ExportFrameworkError, ValidationError
from logger import configure_logging, log_event
from service import ExportService
from validation import validate_request

configure_logging()
LOGGER = logging.getLogger(__name__)


@functions_framework.http
def unified_data_export(request: Request) -> tuple[Any, int]:
    """Validate and execute an authenticated data export request."""
    if request.method != "POST":
        return jsonify({"status": "error", "error": "POST is required"}), 405
    try:
        export_request = validate_request(request.get_json(silent=True))
        payload, status = ExportService().execute(export_request)
        return jsonify({"status": "success", **payload}), status
    except ValidationError as exc:
        log_event(LOGGER, logging.WARNING, "request_rejected", error=str(exc))
        return jsonify({"status": "error", "error": str(exc)}), 400
    except ExportFrameworkError as exc:
        log_event(LOGGER, logging.ERROR, "export_error", error=str(exc))
        return jsonify({"status": "error", "error": str(exc)}), 500
    except Exception:
        LOGGER.exception("unhandled_export_error")
        return jsonify({"status": "error", "error": "internal server error"}), 500
