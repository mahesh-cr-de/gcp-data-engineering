"""Cloud Storage artifact, audit, and idempotency operations."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from google.api_core import exceptions as google_exceptions
from google.api_core.retry import Retry, if_exception_type
from google.cloud import storage

from exceptions import StorageError

GCS_RETRY = Retry(
    predicate=if_exception_type(
        google_exceptions.TooManyRequests,
        google_exceptions.InternalServerError,
        google_exceptions.ServiceUnavailable,
    ),
    deadline=120.0,
)


class GCSRepository:
    """Persist exports and terminal audit manifests in Cloud Storage."""

    def __init__(self, client: storage.Client | None = None) -> None:
        self._client = client or storage.Client()

    def completed_audit(
        self, bucket_name: str, audit_object: str, idempotency_key: str
    ) -> dict[str, Any] | None:
        """Return a matching successful manifest, if one already exists."""
        blob = self._client.bucket(bucket_name).blob(audit_object)
        try:
            payload = json.loads(blob.download_as_text(retry=GCS_RETRY))
        except google_exceptions.NotFound:
            return None
        except Exception as exc:
            raise StorageError(f"failed to read idempotency manifest: {exc}") from exc
        if payload.get("idempotency_key") == idempotency_key and payload.get("status") == "SUCCESS":
            return payload
        return None

    def upload_file(
        self,
        local_path: Path,
        bucket_name: str,
        object_name: str,
        overwrite: bool,
        metadata: dict[str, str],
        timeout: float,
    ) -> str:
        """Upload a local artifact, conditionally protecting existing objects."""
        blob = self._client.bucket(bucket_name).blob(object_name)
        blob.metadata = metadata
        try:
            if not overwrite and blob.exists(retry=GCS_RETRY):
                blob.reload(retry=GCS_RETRY)
                if (blob.metadata or {}).get("idempotency_key") == metadata.get("idempotency_key"):
                    return f"gs://{bucket_name}/{object_name}"
                raise StorageError("GCS object already exists and belongs to another request")
            blob.upload_from_filename(
                str(local_path),
                content_type="application/gzip" if local_path.suffix == ".gz" else "text/csv",
                if_generation_match=None if overwrite else 0,
                retry=GCS_RETRY,
                timeout=timeout,
            )
        except google_exceptions.PreconditionFailed as exc:
            raise StorageError("GCS object already exists and overwrite is false") from exc
        except StorageError:
            raise
        except Exception as exc:
            raise StorageError(f"GCS upload failed: {exc}") from exc
        return f"gs://{bucket_name}/{object_name}"

    def write_audit(self, bucket_name: str, object_name: str, payload: dict[str, Any]) -> None:
        """Write the final audit/idempotency manifest as JSON."""
        try:
            self._client.bucket(bucket_name).blob(object_name).upload_from_string(
                json.dumps(payload, default=str),
                content_type="application/json",
                retry=GCS_RETRY,
            )
        except Exception as exc:
            raise StorageError(f"audit manifest upload failed: {exc}") from exc
