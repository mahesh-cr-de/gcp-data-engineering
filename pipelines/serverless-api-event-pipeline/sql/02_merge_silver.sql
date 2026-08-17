MERGE `${PROJECT_ID}.api_pipeline.api_records_silver` AS target
USING (
  SELECT * EXCEPT(row_number)
  FROM (
    SELECT
      record_id,
      source,
      payload,
      ingested_at,
      event_id,
      ROW_NUMBER() OVER (
        PARTITION BY record_id ORDER BY ingested_at DESC, event_id DESC
      ) AS row_number
    FROM `${PROJECT_ID}.api_pipeline.api_events_staging`
    WHERE ingested_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
  )
  WHERE row_number = 1
) AS source
ON target.record_id = source.record_id
WHEN MATCHED AND target.event_id != source.event_id THEN
  UPDATE SET
    source = source.source,
    payload = source.payload,
    updated_at = source.ingested_at,
    event_id = source.event_id
WHEN NOT MATCHED THEN
  INSERT (record_id, source, payload, first_seen_at, updated_at, event_id)
  VALUES (
    source.record_id, source.source, source.payload,
    source.ingested_at, source.ingested_at, source.event_id
  );

