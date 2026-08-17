# GCP Beam ETL Medallion Architecture

End-to-end batch ETL pipeline using **Apache Beam** on **Google Cloud Dataflow**.

## Overview
- **Source**: JSON/CSV files in GCS (e.g., raw sales data).
- **Bronze**: Raw/ minimally processed data in BigQuery.
- **Silver**: Cleaned, enriched, deduplicated.
- **Gold**: Aggregated, business-ready tables.
- Features: Incremental loads (timestamp watermark), error handling, schema validation, logging.

This demonstrates production-grade patterns for Data Engineering Manager portfolios.

## Architecture
```mermaid
graph TD
    A[GCS Raw Files] --> B[Dataflow Pipeline]
    B --> C[BigQuery Bronze]
    C --> D[Dataform / SQL Transformations]
    D --> E[BigQuery Silver/Gold]