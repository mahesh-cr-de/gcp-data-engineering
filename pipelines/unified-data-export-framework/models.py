"""Domain models shared by the export framework."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True, slots=True)
class ExportRequest:
    """Validated export parameters."""

    job_name: str
    sql_query: str
    project_id: str
    bucket: str
    gcs_path: str
    file_name: str
    delimiter: str
    header: bool
    compression: str | None
    sftp_host: str
    sftp_port: int
    sftp_username: str
    remote_path: str
    timeout: int
    overwrite: bool
    idempotency_key: str
    sftp_password: str | None = None
    sftp_secret: str | None = None
    sftp_private_key_secret: str | None = None
    sftp_host_key: str | None = None


@dataclass(slots=True)
class AuditRecord:
    """Structured audit payload written to logs and Cloud Storage."""

    job_name: str
    idempotency_key: str
    start_time: datetime
    end_time: datetime | None = None
    duration_seconds: float | None = None
    rows_exported: int = 0
    file_size: int = 0
    destination: str = ""
    status: str = "RUNNING"
    error_message: str | None = None

    def finish(self, status: str, error_message: str | None = None) -> None:
        """Mark the audit record complete."""
        self.end_time = datetime.now().astimezone()
        self.duration_seconds = round(
            (self.end_time - self.start_time).total_seconds(), 3
        )
        self.status = status
        self.error_message = error_message

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""
        value = asdict(self)
        value["start_time"] = self.start_time.isoformat()
        value["end_time"] = self.end_time.isoformat() if self.end_time else None
        return value
