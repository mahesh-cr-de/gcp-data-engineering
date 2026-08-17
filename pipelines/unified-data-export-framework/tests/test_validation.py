"""Validation contract tests."""

import pytest

from exceptions import ValidationError
from validation import validate_request


def valid_body() -> dict:
    return {
        "job_name": "vendor_sales",
        "sql_query": "SELECT 1 AS value",
        "project_id": "my-project",
        "bucket": "exports-prod",
        "gcs_path": "vendor/daily",
        "file_name": "sales_{{YYYYMMDD}}.csv",
        "delimiter": ",",
        "header": True,
        "compression": "gzip",
        "sftp_host": "sftp.example.com",
        "sftp_port": 22,
        "sftp_username": "vendor",
        "sftp_secret": "vendor-password",
        "remote_path": "/incoming",
        "timeout": 600,
        "overwrite": False,
    }


def test_valid_request_uses_secret_manager() -> None:
    request = validate_request(valid_body())
    assert request.delimiter == ","
    assert request.sftp_secret == "vendor-password"
    assert len(request.idempotency_key) == 64


@pytest.mark.parametrize("delimiter", [";", "::", " "])
def test_rejects_unsupported_delimiter(delimiter: str) -> None:
    body = valid_body()
    body["delimiter"] = delimiter
    with pytest.raises(ValidationError, match="delimiter"):
        validate_request(body)


def test_inline_password_is_disabled_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    body = valid_body()
    body.pop("sftp_secret")
    body["sftp_password"] = "never-log-me"
    monkeypatch.delenv("ALLOW_INLINE_SFTP_PASSWORD", raising=False)
    with pytest.raises(ValidationError, match="inline"):
        validate_request(body)


def test_rejects_idempotency_path_traversal() -> None:
    body = valid_body()
    body["idempotency_key"] = "../another-job"
    with pytest.raises(ValidationError, match="idempotency_key"):
        validate_request(body)
