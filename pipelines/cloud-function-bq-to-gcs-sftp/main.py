"""HTTP entry point and orchestration for the BigQuery export pipeline."""

import logging
import os
import tempfile

import functions_framework

from config import get_config
from source import run_source_query
from target import download_gcs_blob, export_table_to_gcs, upload_to_sftp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@functions_framework.http
def bq_to_gcs_sftp(request):
    """Run SQL -> export its result to GCS -> relay the file to SFTP."""
    try:
        body = request.get_json(silent=True) or {}
        file_name = body.get("file_name", "export.csv")
        start_date = body.get("start_date")
        end_date = body.get("end_date")

        if not start_date or not end_date:
            return ({"error": "start_date and end_date are required (YYYY-MM-DD)"}, 400)
        if os.path.basename(file_name) != file_name:
            return ({"error": "file_name must not contain a directory path"}, 400)

        cfg = get_config()
        blob_path = f"exports/{file_name}"

        # 1. Source: load the external SQL and run it in BigQuery.
        result_table = run_source_query(cfg, start_date, end_date)

        # 2. Target: export the query result from BigQuery to GCS.
        gcs_uri = export_table_to_gcs(
            cfg["bq_project_id"], result_table, cfg["gcs_bucket"], blob_path
        )

        # 3. Target: stage the GCS object locally and upload it to SFTP.
        local_path = os.path.join(tempfile.gettempdir(), file_name)
        download_gcs_blob(cfg["gcs_bucket"], blob_path, local_path)
        upload_to_sftp(cfg, local_path, file_name)

        return ({"status": "success", "gcs_uri": gcs_uri}, 200)
    except Exception as exc:
        logger.exception("ETL job failed")
        return ({"error": str(exc)}, 500)
