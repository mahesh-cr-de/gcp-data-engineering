CREATE SCHEMA IF NOT EXISTS `${PROJECT_ID}.api_pipeline`
OPTIONS(location = '${REGION}');

CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.api_pipeline.api_events_staging` (
  event_id STRING,
  record_id STRING,
  source STRING,
  ingested_at TIMESTAMP,
  payload JSON
)
PARTITION BY DATE(ingested_at)
CLUSTER BY record_id;

CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.api_pipeline.api_records_silver` (
  record_id STRING NOT NULL,
  source STRING,
  payload JSON,
  first_seen_at TIMESTAMP,
  updated_at TIMESTAMP,
  event_id STRING
)
CLUSTER BY record_id;

