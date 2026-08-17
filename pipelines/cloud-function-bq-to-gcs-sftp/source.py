"""BigQuery source operations."""

import re
from pathlib import Path

from google.cloud import bigquery

SQL_FILE = Path(__file__).parent / "queries" / "export_orders.sql"
_TABLE_ID = re.compile(r"^[A-Za-z0-9_-]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+$")


def _table_id(value: str, config_key: str) -> str:
    """Allow only a fully qualified BigQuery table identifier in SQL templates."""
    if not _TABLE_ID.fullmatch(value):
        raise ValueError(f"Secret field {config_key} must be project.dataset.table")
    return value


def run_source_query(cfg: dict, start_date: str, end_date: str):
    """Read the SQL file, execute it, and return its anonymous result table."""
    sql = SQL_FILE.read_text(encoding="utf-8").format(
        orders_table=_table_id(cfg["source_orders_table"], "source_orders_table"),
        customers_table=_table_id(cfg["source_customers_table"], "source_customers_table"),
        products_table=_table_id(cfg["source_products_table"], "source_products_table"),
    )
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        ]
    )
    job = bigquery.Client(project=cfg["bq_project_id"]).query(sql, job_config=job_config)
    job.result()
    return job.destination
