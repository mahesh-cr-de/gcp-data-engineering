"""Use-case orchestration independent of the HTTP framework."""

from __future__ import annotations

import logging
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from bigquery import BigQueryExporter
from gcs import GCSRepository
from logger import log_event
from models import AuditRecord, ExportRequest
from sftp import SFTPUploader
from utils import object_name, remaining_seconds, remote_file_path, render_file_name

LOGGER = logging.getLogger(__name__)


class ExportService:
    """Coordinate idempotency, extraction, delivery, and auditing."""

    def __init__(
        self,
        exporter: BigQueryExporter | None = None,
        gcs: GCSRepository | None = None,
        sftp: SFTPUploader | None = None,
    ) -> None:
        self._exporter = exporter or BigQueryExporter()
        self._gcs = gcs or GCSRepository()
        self._sftp = sftp or SFTPUploader()

    def execute(self, request: ExportRequest) -> tuple[dict[str, Any], int]:
        """Execute one export and return an API payload plus HTTP status."""
        start = datetime.now().astimezone()
        deadline = time.monotonic() + request.timeout
        name = render_file_name(request.file_name, start)
        if request.compression and not name.endswith(".gz"):
            name += ".gz"
        gcs_object = object_name(request.gcs_path, name)
        remote_object = remote_file_path(request.remote_path, name)
        audit_object = object_name(
            request.gcs_path, f"_audit/{request.job_name}/{request.idempotency_key}.json"
        )
        existing = self._gcs.completed_audit(
            request.bucket, audit_object, request.idempotency_key
        )
        if existing:
            return {"message": "request already completed", "audit": existing}, 200

        audit = AuditRecord(
            job_name=request.job_name,
            idempotency_key=request.idempotency_key,
            start_time=start,
            destination=f"gs://{request.bucket}/{gcs_object}; sftp://{request.sftp_host}{remote_object}",
        )
        log_event(LOGGER, logging.INFO, "export_started", **audit.to_dict())
        try:
            with tempfile.TemporaryDirectory(prefix="export-") as directory:
                local_path = Path(directory, name)
                audit.rows_exported = self._exporter.export_to_file(
                    request, local_path, deadline
                )
                audit.file_size = local_path.stat().st_size
                gcs_uri = self._gcs.upload_file(
                    local_path,
                    request.bucket,
                    gcs_object,
                    request.overwrite,
                    {
                        "job_name": request.job_name,
                        "idempotency_key": request.idempotency_key,
                    },
                    min(remaining_seconds(deadline, 5), 120),
                )
                self._sftp.upload(request, local_path, remote_object, deadline)
            audit.finish("SUCCESS")
            self._gcs.write_audit(request.bucket, audit_object, audit.to_dict())
            log_event(LOGGER, logging.INFO, "export_succeeded", **audit.to_dict())
            return {"message": "export completed", "gcs_uri": gcs_uri, "audit": audit.to_dict()}, 200
        except Exception as exc:
            audit.finish("FAILED", str(exc))
            log_event(LOGGER, logging.ERROR, "export_failed", **audit.to_dict())
            try:
                self._gcs.write_audit(request.bucket, audit_object, audit.to_dict())
            except Exception as audit_exc:
                log_event(LOGGER, logging.ERROR, "audit_write_failed", error=str(audit_exc))
            raise
