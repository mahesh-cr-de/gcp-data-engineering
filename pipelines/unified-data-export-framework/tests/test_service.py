"""Orchestration and replay tests using in-memory collaborators."""

from pathlib import Path

from service import ExportService
from test_validation import valid_body
from validation import validate_request


class Exporter:
    def export_to_file(self, request, destination: Path, deadline: float) -> int:
        destination.write_bytes(b"id\n1\n")
        return 1


class GCS:
    def __init__(self, existing=None):
        self.existing = existing
        self.audit = None

    def completed_audit(self, bucket, name, key):
        return self.existing

    def upload_file(self, local_path, bucket, name, overwrite, metadata, timeout):
        return f"gs://{bucket}/{name}"

    def write_audit(self, bucket, name, payload):
        self.audit = payload


class SFTP:
    def __init__(self):
        self.called = False

    def upload(self, request, local_path, remote_path, deadline):
        self.called = True


def test_success_writes_terminal_audit() -> None:
    gcs = GCS()
    sftp = SFTP()
    payload, status = ExportService(Exporter(), gcs, sftp).execute(
        validate_request(valid_body())
    )
    assert status == 200
    assert payload["audit"]["status"] == "SUCCESS"
    assert gcs.audit["rows_exported"] == 1
    assert sftp.called


def test_completed_idempotency_key_skips_work() -> None:
    existing = {"status": "SUCCESS", "idempotency_key": "abc"}
    sftp = SFTP()
    payload, status = ExportService(Exporter(), GCS(existing), sftp).execute(
        validate_request(valid_body())
    )
    assert status == 200
    assert "already completed" in payload["message"]
    assert not sftp.called
