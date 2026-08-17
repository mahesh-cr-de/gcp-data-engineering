"""Small, side-effect-free framework utilities."""

from __future__ import annotations

import os
import re
import time
from datetime import datetime
from pathlib import PurePosixPath

from exceptions import ExportTimeoutError, ValidationError

_SAFE_FILE = re.compile(r"^[A-Za-z0-9_.{}-]+$")


def render_file_name(template: str, now: datetime | None = None) -> str:
    """Replace supported date placeholders in a safe file name."""
    current = now or datetime.now().astimezone()
    values = {
        "{{YYYYMMDD}}": current.strftime("%Y%m%d"),
        "{{YYYY-MM-DD}}": current.strftime("%Y-%m-%d"),
        "{{HHMMSS}}": current.strftime("%H%M%S"),
        "{{timestamp}}": str(int(current.timestamp())),
    }
    if not _SAFE_FILE.fullmatch(template) or "/" in template or "\\" in template:
        raise ValidationError("file_name must be a safe base name without path separators")
    for placeholder, value in values.items():
        template = template.replace(placeholder, value)
    if "{{" in template or "}}" in template:
        raise ValidationError("file_name contains an unsupported placeholder")
    return template


def object_name(prefix: str, file_name: str) -> str:
    """Join a normalized GCS prefix and file name."""
    normalized = prefix.strip("/")
    return str(PurePosixPath(normalized, file_name)) if normalized else file_name


def remote_file_path(remote_path: str, file_name: str) -> str:
    """Join a POSIX SFTP directory and file name."""
    return str(PurePosixPath(remote_path, file_name))


def remaining_seconds(deadline: float, reserve: float = 0.0) -> float:
    """Return remaining monotonic time or raise a domain timeout."""
    remaining = deadline - time.monotonic() - reserve
    if remaining <= 0:
        raise ExportTimeoutError("export execution deadline exceeded")
    return remaining


def max_temp_bytes() -> int:
    """Return the configured local-file safety limit."""
    return int(os.getenv("MAX_TEMP_FILE_BYTES", str(4 * 1024**3)))
