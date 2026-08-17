"""Request parsing and validation at the HTTP boundary."""

from __future__ import annotations

import hashlib
import os
import re
from typing import Any, Mapping

from exceptions import ValidationError
from models import ExportRequest

_JOB_NAME = re.compile(r"^[A-Za-z0-9_-]{1,100}$")
_IDEMPOTENCY_KEY = re.compile(r"^[A-Za-z0-9_.-]{1,200}$")
_BUCKET = re.compile(r"^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$")
_ALLOWED_DELIMITERS = {",", "\t", "|"}


def _required_text(body: Mapping[str, Any], name: str) -> str:
    value = body.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{name} is required and must be a non-empty string")
    return value.strip()


def _boolean(body: Mapping[str, Any], name: str, default: bool) -> bool:
    value = body.get(name, default)
    if not isinstance(value, bool):
        raise ValidationError(f"{name} must be a boolean")
    return value


def validate_request(body: Any) -> ExportRequest:
    """Validate untrusted JSON and construct an immutable domain request."""
    if not isinstance(body, dict):
        raise ValidationError("request body must be a JSON object")

    job_name = _required_text(body, "job_name")
    if not _JOB_NAME.fullmatch(job_name):
        raise ValidationError("job_name may contain only letters, numbers, _ and -")
    sql_query = _required_text(body, "sql_query")
    if ";" in sql_query.rstrip().rstrip(";"):
        raise ValidationError("multi-statement SQL is not supported")
    project_id = str(body.get("project_id") or os.getenv("GOOGLE_CLOUD_PROJECT", "")).strip()
    if not project_id:
        raise ValidationError("project_id is required or GOOGLE_CLOUD_PROJECT must be set")
    bucket = _required_text(body, "bucket")
    if not _BUCKET.fullmatch(bucket):
        raise ValidationError("bucket is not a valid Cloud Storage bucket name")

    delimiter = body.get("delimiter", ",")
    if delimiter == "\\t":
        delimiter = "\t"
    if delimiter not in _ALLOWED_DELIMITERS:
        raise ValidationError("delimiter must be comma, tab, or pipe")
    compression_value = body.get("compression")
    compression = None if compression_value in (None, "", "none") else compression_value
    if compression != "gzip":
        raise ValidationError("compression must be gzip, none, or null")

    port = body.get("sftp_port", 22)
    timeout = body.get("timeout", 600)
    if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
        raise ValidationError("sftp_port must be an integer from 1 to 65535")
    max_timeout = int(os.getenv("MAX_REQUEST_TIMEOUT", "3500"))
    if not isinstance(timeout, int) or isinstance(timeout, bool) or not 30 <= timeout <= max_timeout:
        raise ValidationError(f"timeout must be an integer from 30 to {max_timeout}")

    password = body.get("sftp_password")
    password_secret = body.get("sftp_secret")
    key_secret = body.get("sftp_private_key_secret")
    if sum(bool(value) for value in (password, password_secret, key_secret)) != 1:
        raise ValidationError(
            "provide exactly one of sftp_password, sftp_secret, or sftp_private_key_secret"
        )
    if password and not os.getenv("ALLOW_INLINE_SFTP_PASSWORD", "false").lower() == "true":
        raise ValidationError("inline sftp_password is disabled; use Secret Manager")

    raw_key = body.get("idempotency_key")
    if raw_key is not None and (
        not isinstance(raw_key, str) or not _IDEMPOTENCY_KEY.fullmatch(raw_key)
    ):
        raise ValidationError(
            "idempotency_key may contain only letters, numbers, dot, _ and -"
        )
    generated_key = hashlib.sha256(
        f"{job_name}|{sql_query}|{body.get('file_name', '')}".encode()
    ).hexdigest()

    return ExportRequest(
        job_name=job_name,
        sql_query=sql_query,
        project_id=project_id,
        bucket=bucket,
        gcs_path=str(body.get("gcs_path", "")).strip(),
        file_name=_required_text(body, "file_name"),
        delimiter=delimiter,
        header=_boolean(body, "header", True),
        compression=compression,
        sftp_host=_required_text(body, "sftp_host"),
        sftp_port=port,
        sftp_username=_required_text(body, "sftp_username"),
        remote_path=_required_text(body, "remote_path"),
        timeout=timeout,
        overwrite=_boolean(body, "overwrite", False),
        idempotency_key=raw_key or generated_key,
        sftp_password=password,
        sftp_secret=password_secret,
        sftp_private_key_secret=key_secret,
        sftp_host_key=body.get("sftp_host_key"),
    )
