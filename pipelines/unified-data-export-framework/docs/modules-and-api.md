# Request contract and module reference

## Folder structure

```text
unified-data-export-framework/
|-- main.py                         HTTP entry point
|-- service.py                      ordered export use case
|-- models.py                       request and audit domain models
|-- exceptions.py                   exception hierarchy
|-- bigquery.py                     query and streaming serializer
|-- gcs.py                          artifact, audit, and replay repository
|-- sftp.py                         secrets, host verification, upload
|-- validation.py                   untrusted input validation
|-- logger.py                       structured JSON logging
|-- utils.py                        filename, path, deadline helpers
|-- requirements.txt                runtime dependencies
|-- requirements-dev.txt            pytest dependencies
|-- tests/                          isolated unit tests
`-- docs/                           architecture and runbooks
```

## Request example

```json
{
  "job_name": "vendor_sales",
  "idempotency_key": "vendor_sales_2026-07-04",
  "sql_query": "SELECT * FROM `my-project.dataset.sales` WHERE business_date = CURRENT_DATE()",
  "project_id": "my-project",
  "bucket": "exports-prod",
  "gcs_path": "vendor1/daily/",
  "file_name": "sales_{{YYYYMMDD}}.csv",
  "delimiter": ",",
  "header": true,
  "compression": "gzip",
  "sftp_host": "host.company.com",
  "sftp_port": 22,
  "sftp_username": "vendoruser",
  "sftp_secret": "vendor1-sftp-password",
  "sftp_host_key": "host.company.com ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAA...",
  "remote_path": "/incoming/",
  "timeout": 600,
  "overwrite": false
}
```

`delimiter` accepts `,`, `|`, a literal tab, or the JSON string `"\\t"`.
`compression` accepts `"gzip"`, `"none"`, `null`, or an empty string. Gzip
automatically adds `.gz` when absent. Supported filename tokens are
`{{YYYYMMDD}}`, `{{YYYY-MM-DD}}`, `{{HHMMSS}}`, and `{{timestamp}}`.

Exactly one credential input is required:

| Field | Meaning |
|---|---|
| `sftp_secret` | Secret Manager password secret; recommended for passwords |
| `sftp_private_key_secret` | Secret Manager PEM private-key secret; recommended |
| `sftp_password` | Direct password; accepted only with the explicit compatibility environment flag |

A short secret name resolves in `project_id`. A full
`projects/.../secrets/.../versions/...` resource is also accepted.

## Response examples

Successful execution returns HTTP 200:

```json
{
  "status": "success",
  "message": "export completed",
  "gcs_uri": "gs://exports-prod/vendor1/daily/sales_20260704.csv.gz",
  "audit": {
    "job_name": "vendor_sales",
    "status": "SUCCESS",
    "rows_exported": 1250043,
    "file_size": 48200391,
    "error_message": null
  }
}
```

Validation failures return 400, non-POST requests return 405, and expected
execution failures return 500. Unhandled failures return a generic message so
credentials or SDK internals cannot leak to callers.

## Every module explained

- `main.py` is the Functions Framework adapter. It enforces POST, builds the
  domain request, invokes the service, and maps errors to JSON HTTP responses.
- `service.py` is the application use case. It sequences replay check, local
  creation, GCS delivery, SFTP delivery, and terminal auditing.
- `models.py` contains typed, framework-independent data structures. The audit
  model owns duration calculation and JSON-safe conversion.
- `exceptions.py` defines a common base plus validation, query, storage, SFTP,
  secret, and timeout errors.
- `bigquery.py` submits Standard SQL with transient API retry, waits within the
  request deadline, reads through BigQuery Storage API, and writes batches with
  Python's standards-compliant CSV writer. PyArrow is used only at the provider
  boundary; pandas is intentionally absent.
- `gcs.py` uploads with generation preconditions, preserves idempotency metadata,
  checks completed manifests, and writes audit JSON.
- `sftp.py` resolves secrets with ADC, verifies the server's exact host key,
  supports password and Ed25519/RSA/ECDSA keys, retries transient SSH failures,
  and uses a partial-file rename.
- `validation.py` rejects missing fields, invalid types, unsafe names, unsupported
  delimiters/compression, multiple credential modes, multi-statement SQL, and
  excessive caller deadlines.
- `logger.py` emits structured JSON compatible with Cloud Logging without a
  second logging transport or background flush requirement.
- `utils.py` safely renders filenames, joins POSIX/GCS paths, enforces monotonic
  deadlines, and reads the local file safety limit.
- `tests/` uses injected fakes and small Arrow batches; no cloud account or SFTP
  server is needed.
