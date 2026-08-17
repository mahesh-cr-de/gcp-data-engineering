"""Bounded-batch file serialization tests."""

import gzip
import time
from dataclasses import replace
from pathlib import Path

import pyarrow as pa

from bigquery import BigQueryExporter
from test_validation import valid_body
from validation import validate_request


def test_writes_gzip_csv_batch_by_batch(tmp_path: Path) -> None:
    request = validate_request(valid_body())
    batches = iter(
        [
            pa.record_batch([[1, 2], ["a", "b"]], names=["id", "name"]),
            pa.record_batch([[3], ["c"]], names=["id", "name"]),
        ]
    )
    target = tmp_path / "output.csv.gz"
    rows = BigQueryExporter._write_batches(batches, request, target, time.monotonic() + 10)
    assert rows == 3
    with gzip.open(target, "rt", encoding="utf-8") as handle:
        assert handle.read() == "id,name\n1,a\n2,b\n3,c\n"


def test_writes_tsv_without_header(tmp_path: Path) -> None:
    request = replace(validate_request(valid_body()), delimiter="\t", header=False, compression=None)
    target = tmp_path / "output.tsv"
    BigQueryExporter._write_batches(
        iter([pa.record_batch([[1], ["a"]], names=["id", "name"])]),
        request,
        target,
        time.monotonic() + 10,
    )
    assert target.read_text(encoding="utf-8") == "1\ta\n"


def test_empty_result_still_writes_header(tmp_path: Path) -> None:
    request = replace(validate_request(valid_body()), compression=None)
    target = tmp_path / "empty.csv"
    rows = BigQueryExporter._write_batches(
        iter([]), request, target, time.monotonic() + 10, field_names=["id", "name"]
    )
    assert rows == 0
    assert target.read_text(encoding="utf-8") == "id,name\n"
