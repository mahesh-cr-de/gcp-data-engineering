# Local testing, Postman, and operations

## Unit tests

Python 3.12 and Application Default Credentials are only needed for live calls;
the unit suite itself uses fakes.

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements-dev.txt
pytest -q --cov=. --cov-report=term-missing
```

## Run locally

```powershell
gcloud auth application-default login
$env:GOOGLE_CLOUD_PROJECT = "my-project"
functions-framework --target unified_data_export --port 8080 --debug
```

In another terminal:

```powershell
$body = Get-Content .\scheduler-body.json -Raw
Invoke-RestMethod -Method Post -Uri http://localhost:8080 `
  -ContentType application/json -Body $body
```

Do not commit `scheduler-body.json` if it contains a direct password. The safer
local path is `sftp_secret` plus ADC with secret accessor permission.

## Postman example

1. Create a `POST` request to the deployed function URL.
2. Under Authorization choose **Bearer Token** and paste the output of
   `gcloud auth print-identity-token` for an identity with invoker permission.
3. Set `Content-Type: application/json`.
4. Choose Body > raw > JSON and paste the request from
   [modules-and-api.md](modules-and-api.md).
5. Send. Expect HTTP 200 and an audit object in GCS. A replay with the same
   idempotency key returns `request already completed`.

Never store a long-lived token or password in a shared Postman collection.

## Cloud Logging queries

```text
resource.type="cloud_run_revision"
jsonPayload.job_name="vendor_sales"
jsonPayload.status="FAILED"
```

Create log-based metrics for success, failure, duration, rows, and file size.
Alert on failures, missing scheduled success, duration near the timeout, and
temp-file guard failures.

## Failure and retry behavior

- BigQuery, Storage, Secret Manager, and transient SSH transport failures use
  bounded exponential retries.
- Every blocking phase receives the remaining monotonic deadline. A query job is
  cancelled when its wait times out.
- Failures emit structured logs and attempt to persist a failed audit manifest.
- Scheduler retries should reuse the same idempotency key.
- If SFTP fails after GCS succeeds, a retry recognizes matching GCS metadata,
  regenerates the local file, and resumes SFTP delivery.

## Production checklist

- Pin and review dependencies through an automated lock/update process.
- Restrict invocation and source datasets with IAM; do not expose arbitrary SQL
  to general API clients.
- Store and rotate SFTP credentials in Secret Manager.
- Obtain the host key over a trusted channel and test rotation before cutover.
- Load-test row width, local bytes, timeout, and instance count.
- Define lifecycle rules for artifacts and `_audit/` manifests.
- Confirm null, delimiter, newline, timestamp, decimal, encoding, header, gzip,
  and empty-export contracts with every receiving vendor.
