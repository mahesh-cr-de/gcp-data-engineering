# Architecture and production design

## Runtime flow

```text
Cloud Scheduler (OIDC, at-least-once delivery)
    |
    v
Cloud Function Gen2 / unified_data_export
    |-- validate request and derive idempotency key
    |-- check terminal audit manifest in GCS
    |-- submit Standard SQL query to BigQuery
    |-- read Arrow RecordBatches through BigQuery Storage API
    |-- serialize CSV/TSV/pipe data to /tmp, optionally through gzip
    |-- upload the local artifact to GCS
    |-- upload the same artifact to SFTP using a verified SSH host key
    |-- persist terminal audit JSON and emit structured Cloud Logging events
    `-- return JSON
```

## Clean architecture

The HTTP adapter (`main.py`) owns protocol concerns. `validation.py` maps
untrusted JSON into the immutable `ExportRequest` domain model. `ExportService`
implements the use case and depends on focused BigQuery, GCS, and SFTP
collaborators. Provider SDK details stay behind those classes. Domain-specific
exceptions prevent SDK errors from leaking into the public API.

The classes each have one reason to change, accept injected collaborators for
tests, and expose narrow interfaces. Adding another destination therefore does
not require changing query extraction or request parsing.

## Large-result behavior

`QueryJob.result().to_arrow_iterable()` uses the BigQuery Storage API and yields
one Arrow batch at a time. Each batch is converted and written before the next
is requested. Neither pandas nor a full Arrow table is created. Set
`BQ_STORAGE_MAX_STREAMS=1` to preserve deterministic row delivery with minimal
memory pressure; increase it only after measuring memory and ordering needs.

The complete output must still fit in the function's temporary filesystem.
`MAX_TEMP_FILE_BYTES` stops the job before uncontrolled growth. A single Gen2
invocation is limited to 3,600 seconds, so exports that cannot reliably fit the
configured local limit and deadline belong in Cloud Run Jobs or Dataflow. This
framework supports millions of rows when their serialized artifact fits those
boundaries; row count alone is not the sizing unit.

## Idempotency and delivery semantics

Cloud Scheduler is at-least-once. The caller should send a stable
`idempotency_key` for one business export. If omitted, the framework hashes job
name, SQL, and filename template. A successful audit manifest at
`<gcs_path>/_audit/<job_name>/<idempotency_key>.json` short-circuits replays.

GCS creates are generation-guarded when `overwrite=false`; a matching
idempotency metadata value makes a partial replay safe. SFTP first uploads to a
request-specific `.part` name and then renames it. A same-sized final remote
file is treated as a completed partial replay. This is practical at-least-once
idempotency, not a distributed transaction: consumers should ingest files
atomically and de-duplicate by the business key.

## Security decisions

- The function is private and invoked with Scheduler OIDC.
- SFTP credentials come from Secret Manager; inline passwords are disabled by
  default and are never logged.
- An OpenSSH `known_hosts` line is mandatory, preventing silent man-in-the-middle
  acceptance.
- The runtime service account should have query-job permission, source-dataset
  read permission, object permission only on the export bucket, and accessor
  permission only on the relevant secrets.
- Arbitrary SQL is powerful. Only trusted scheduler identities should invoke
  this function, and the runtime service account must be dataset-scoped. For
  multi-tenant use, replace `sql_query` with an allow-listed query identifier.

## Audit contract

Every terminal record contains job name, idempotency key, start/end timestamps,
duration, rows exported, file size, both destinations, status, and error
message. The same fields are emitted as one-line structured JSON to stdout,
which Cloud Logging parses and indexes.
