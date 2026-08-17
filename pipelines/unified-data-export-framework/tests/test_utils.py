"""Filename and path utility tests."""

from datetime import datetime, timezone

import pytest

from exceptions import ValidationError
from utils import object_name, render_file_name


def test_render_all_placeholders() -> None:
    now = datetime(2026, 7, 4, 9, 8, 7, tzinfo=timezone.utc)
    rendered = render_file_name(
        "x_{{YYYYMMDD}}_{{YYYY-MM-DD}}_{{HHMMSS}}_{{timestamp}}.csv", now
    )
    assert rendered == "x_20260704_2026-07-04_090807_1783156087.csv"


def test_rejects_path_traversal() -> None:
    with pytest.raises(ValidationError):
        render_file_name("../secret.csv")


def test_object_name_normalizes_slashes() -> None:
    assert object_name("/vendor/daily/", "sales.csv") == "vendor/daily/sales.csv"
