"""BigQuery query execution and bounded-memory result serialization."""

from __future__ import annotations

import csv
import base64
import gzip
import json
import logging
import os
import time
from contextlib import nullcontext
from pathlib import Path
from typing import IO, Iterator

import pyarrow as pa
from google.api_core import exceptions as google_exceptions
from google.api_core.retry import Retry, if_exception_type
from google.cloud import bigquery as bq
from google.cloud import bigquery_storage_v1

from exceptions import ExportTimeoutError, QueryExecutionError
from logger import log_event
from models import ExportRequest
from utils import max_temp_bytes, remaining_seconds

LOGGER = logging.getLogger(__name__)
QUERY_RETRY = Retry(
    predicate=if_exception_type(
        google_exceptions.TooManyRequests,
        google_exceptions.InternalServerError,
        google_exceptions.ServiceUnavailable,
    ),
    initial=1.0,
    maximum=20.0,
    multiplier=2.0,
    deadline=120.0,
)


class BigQueryExporter:
    """Run SQL and stream Arrow record batches into a delimited local file."""

    def __init__(
        self,
        client: bq.Client | None = None,
        storage_client: bigquery_storage_v1.BigQueryReadClient | None = None,
    ) -> None:
        self._client = client
        self._storage_client = storage_client

    def export_to_file(
        self, request: ExportRequest, destination: Path, deadline: float
    ) -> int:
        """Execute the query and return the number of serialized rows."""
        client = self._client or bq.Client(project=request.project_id)
        storage = self._storage_client or bigquery_storage_v1.BigQueryReadClient()
        job: bq.QueryJob | None = None
        try:
            job_config = bq.QueryJobConfig(
                use_legacy_sql=False,
                labels={"framework": "unified-export", "job": request.job_name.lower()[:63]},
            )
            job = client.query(
                request.sql_query,
                job_config=job_config,
                retry=QUERY_RETRY,
                timeout=min(remaining_seconds(deadline, 10), 120),
            )
            result = job.result(timeout=remaining_seconds(deadline, 10), retry=QUERY_RETRY)
            batches = result.to_arrow_iterable(
                bqstorage_client=storage,
                max_stream_count=int(os.getenv("BQ_STORAGE_MAX_STREAMS", "1")),
            )
            rows = self._write_batches(
                batches,
                request,
                destination,
                deadline,
                field_names=[field.name for field in result.schema],
            )
            log_event(LOGGER, logging.INFO, "bigquery_export_complete", rows=rows, job_id=job.job_id)
            return rows
        except ExportTimeoutError:
            if job is not None:
                job.cancel()
            raise
        except TimeoutError as exc:
            if job is not None:
                job.cancel()
            raise ExportTimeoutError("BigQuery execution timed out") from exc
        except Exception as exc:
            raise QueryExecutionError(f"BigQuery export failed: {exc}") from exc

    @staticmethod
    def _write_batches(
        batches: Iterator[pa.RecordBatch],
        request: ExportRequest,
        destination: Path,
        deadline: float,
        field_names: list[str] | None = None,
    ) -> int:
        """Serialize one Arrow batch at a time; never materialize the full result."""
        row_count = 0
        raw: IO[bytes]
        with destination.open("wb") as raw:
            compressed = gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) if request.compression else nullcontext(raw)
            with compressed as binary:
                text = __import__("io").TextIOWrapper(binary, encoding="utf-8", newline="")
                try:
                    writer = csv.writer(
                        text,
                        delimiter=request.delimiter,
                        quoting=csv.QUOTE_MINIMAL,
                        lineterminator="\n",
                    )
                    header_written = False
                    if request.header and field_names is not None:
                        writer.writerow(field_names)
                        header_written = True
                    for batch in batches:
                        remaining_seconds(deadline, 5)
                        if request.header and not header_written:
                            writer.writerow(batch.schema.names)
                            header_written = True
                        columns = [
                            [BigQueryExporter._csv_value(value) for value in column.to_pylist()]
                            for column in batch.columns
                        ]
                        writer.writerows(zip(*columns))
                        row_count += batch.num_rows
                        text.flush()
                        if raw.tell() > max_temp_bytes():
                            raise QueryExecutionError("local export exceeded MAX_TEMP_FILE_BYTES")
                finally:
                    text.flush()
                    text.detach()
        return row_count

    @staticmethod
    def _csv_value(value: object) -> object:
        """Normalize nested and binary BigQuery values deterministically."""
        if isinstance(value, (dict, list, tuple)):
            return json.dumps(value, default=str, separators=(",", ":"))
        if isinstance(value, bytes):
            return base64.b64encode(value).decode("ascii")
        return value
