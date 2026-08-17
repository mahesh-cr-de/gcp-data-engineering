"""Cloud Function Gen2: extract paginated API records and publish JSON events."""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import functions_framework
import requests
from google.cloud import pubsub_v1
from google.cloud import secretmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT", "")
TOPIC_ID = os.environ.get("PUBSUB_TOPIC", "api-events")
API_URL = os.environ.get("API_URL", "https://jsonplaceholder.typicode.com/posts")
API_SECRET = os.environ.get("API_SECRET_NAME", "")
PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "100"))
MAX_PAGES = int(os.environ.get("MAX_PAGES", "100"))
REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT_SECONDS", "30"))
PAGE_PARAM = os.environ.get("PAGE_PARAM", "_page")
LIMIT_PARAM = os.environ.get("LIMIT_PARAM", "_limit")


def _api_token() -> str | None:
    """Read an optional bearer token from Secret Manager."""
    if not API_SECRET:
        return None
    name = f"projects/{PROJECT_ID}/secrets/{API_SECRET}/versions/latest"
    response = secretmanager.SecretManagerServiceClient().access_secret_version(
        request={"name": name}
    )
    return response.payload.data.decode("utf-8")


def _records(body: Any) -> list[dict[str, Any]]:
    """Accept either a JSON array or a common {data|results|items: []} response."""
    if isinstance(body, list):
        records = body
    elif isinstance(body, dict):
        records = next(
            (body[key] for key in ("data", "results", "items") if isinstance(body.get(key), list)),
            [],
        )
    else:
        records = []
    if not all(isinstance(item, dict) for item in records):
        raise ValueError("API response contains a non-object record")
    return records


def _event(record: dict[str, Any], ingested_at: str) -> dict[str, Any]:
    record_id = record.get("id") or record.get("record_id")
    if record_id is None:
        raise ValueError("Every API record must contain id or record_id")
    canonical = json.dumps(record, sort_keys=True, separators=(",", ":"), default=str)
    event_id = hashlib.sha256(f"{API_URL}|{record_id}|{canonical}".encode()).hexdigest()
    return {
        "event_id": event_id,
        "record_id": str(record_id),
        "source": API_URL,
        "ingested_at": ingested_at,
        "payload": record,
    }


@functions_framework.http
def ingest_api(request):
    """Convert unexpected setup/runtime failures into observable HTTP errors."""
    try:
        return _ingest_api(request)
    except Exception as exc:  # Framework boundary: log unexpected client/library errors.
        logger.exception("API ingestion failed before completion")
        return ({"status": "error", "error": str(exc)}, 500)


def _ingest_api(request):
    """Scheduler-invoked HTTP endpoint. Publishes one event per API record."""
    if not PROJECT_ID:
        return ({"error": "GOOGLE_CLOUD_PROJECT is not set"}, 500)

    body = request.get_json(silent=True) or {}
    start_page = max(int(body.get("start_page", 1)), 1)
    token = _api_token()
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    futures = []
    published = 0
    pages = 0

    try:
        with requests.Session() as session:
            for page in range(start_page, start_page + MAX_PAGES):
                response = session.get(
                    API_URL,
                    headers=headers,
                    params={PAGE_PARAM: page, LIMIT_PARAM: PAGE_SIZE},
                    timeout=REQUEST_TIMEOUT,
                )
                response.raise_for_status()
                records = _records(response.json())
                pages += 1
                if not records:
                    break

                now = datetime.now(timezone.utc).isoformat()
                for record in records:
                    event = _event(record, now)
                    data = json.dumps(event, separators=(",", ":"), default=str).encode()
                    futures.append(
                        publisher.publish(
                            topic_path,
                            data,
                            event_id=event["event_id"],
                            record_id=event["record_id"],
                        )
                    )
                    published += 1

                # A short page is the portable end-of-pagination signal.
                if len(records) < PAGE_SIZE:
                    break

        for future in futures:
            future.result(timeout=60)
        logger.info("Published %d records from %d pages", published, pages)
        return ({"status": "success", "pages": pages, "published": published}, 200)
    except (requests.RequestException, ValueError, TimeoutError) as exc:
        logger.exception("API ingestion failed")
        return ({"status": "error", "error": str(exc), "published": published}, 500)
