# bq-to-gcs-sftp

Cloud Function that runs an external BigQuery SQL query, exports its result to GCS as a pipe-delimited CSV
(server-side `EXTRACT` job, no rows pulled through function memory), then
relays the file to an SFTP destination.

## Architecture

```
SQL file -> BigQuery query -> GCS bucket -> /tmp -> SFTP host
```

All environment-specific config (target BQ project, destination bucket,
SFTP host/credentials/path) lives in **one Secret Manager secret** as a JSON
blob — not in env vars, not in code. This keeps IAM to a single
`secretAccessor` grant and avoids N separate Secret Manager calls on cold
start.

## Prerequisites

- A GCP project with billing enabled
- `gcloud` CLI authenticated, with `gcloud config set project <PROJECT_ID>`
- APIs enabled:
  ```bash
  gcloud services enable \
    cloudfunctions.googleapis.com \
    cloudbuild.googleapis.com \
    secretmanager.googleapis.com \
    bigquery.googleapis.com \
    storage.googleapis.com \
    run.googleapis.com
  ```
- Network reachability from Cloud Functions Gen2 (Cloud Run-based) to your
  SFTP host. If the SFTP server is on a private network/VPC, attach a
  [Serverless VPC Access connector](https://cloud.google.com/functions/docs/networking/connecting-vpc)
  to the deployment — see the **Deploy** section.

---

## 1. One-time setup

### 1.1 Create the config secret

```bash
cat > config.json <<'EOF'
{
  "bq_project_id": "my-bq-project",
  "source_orders_table": "my-bq-project.sales.orders",
  "source_customers_table": "my-bq-project.sales.customers",
  "source_products_table": "my-bq-project.sales.products",
  "gcs_bucket": "my-export-bucket",
  "sftp_host": "sftp.example.com",
  "sftp_port": 22,
  "sftp_username": "etl_user",
  "sftp_password": "change-me",
  "sftp_private_key": "",
  "sftp_remote_path": "/inbound/"
}
EOF

gcloud secrets create bq-export-etl-config --data-file=config.json
rm config.json   # don't leave plaintext creds on disk
```

To rotate later (new version, old one stays for rollback):
```bash
gcloud secrets versions add bq-export-etl-config --data-file=config.json
```

**Auth options inside the secret:**
| Field | Use when |
|---|---|
| `sftp_password` | Username/password auth |
| `sftp_private_key` | Key-based auth — paste the PEM private key as a string; leave `sftp_password` empty |

### 1.2 Service account and IAM

Use a dedicated runtime SA rather than the default compute SA:

```bash
gcloud iam service-accounts create bq-sftp-exporter \
  --display-name="BQ to GCS/SFTP exporter"

SA_EMAIL="bq-sftp-exporter@$(gcloud config get-value project).iam.gserviceaccount.com"

# Secret access
gcloud secrets add-iam-policy-binding bq-export-etl-config \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/secretmanager.secretAccessor"

# BigQuery — scope to the dataset, not project-wide, if possible
gcloud projects add-iam-policy-binding my-bq-project \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.dataViewer"
gcloud projects add-iam-policy-binding my-bq-project \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.jobUser"

# GCS — scope to the bucket
gsutil iam ch \
  serviceAccount:${SA_EMAIL}:roles/storage.objectAdmin \
  gs://my-export-bucket
```

| Role | Scope | Why |
|---|---|---|
| `roles/secretmanager.secretAccessor` | the one secret | read config at runtime |
| `roles/bigquery.dataViewer` | source dataset | read table data |
| `roles/bigquery.jobUser` | source project | run the EXTRACT job |
| `roles/storage.objectAdmin` | destination bucket | write extract, read it back |

---

## 2. Deploy

```bash
gcloud functions deploy bq-to-gcs-sftp \
  --gen2 \
  --runtime=python312 \
  --region=us-central1 \
  --source=. \
  --entry-point=bq_to_gcs_sftp \
  --trigger-http \
  --no-allow-unauthenticated \
  --service-account="${SA_EMAIL}" \
  --memory=512Mi \
  --timeout=300s \
  --set-env-vars=CONFIG_SECRET_NAME=bq-export-etl-config
```

Notes:
- `--no-allow-unauthenticated` — invoke via an identity token (service-to-service or Cloud Scheduler with OIDC), not anonymously.
- `--memory=512Mi` / `--timeout=300s` — sized for moderate exports. Bump both for large files (extract is server-side, but the GCS→/tmp→SFTP hop still moves the full file through the function).
- If the SFTP host is on a private network, add:
  ```bash
  --vpc-connector=<your-connector> --egress-settings=private-ranges-only
  ```
- If `sftp_private_key` is set, no extra deploy flags needed — it's read from the secret payload at runtime.

### Per-environment configs (dev/stage/prod)

Use separate secrets and deployments, e.g. `bq-export-etl-config-dev`,
`bq-export-etl-config-prod`, pointed to by `CONFIG_SECRET_NAME` per
environment — keeps the same `main.py` portable across environments.

---

## 3. Invoke

### Manually (for testing)

```bash
FUNCTION_URL=$(gcloud functions describe bq-to-gcs-sftp \
  --region=us-central1 --gen2 --format="value(serviceConfig.uri)")

curl -X POST "${FUNCTION_URL}" \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -H "Content-Type: application/json" \
  -d '{"start_date":"2026-06-01","end_date":"2026-06-29","file_name":"daily_orders_20260629.csv"}'
```

Request body:
| Field | Required | Description |
|---|---|---|
| `start_date` | yes | Inclusive query start date in `YYYY-MM-DD` format |
| `end_date` | yes | Inclusive query end date in `YYYY-MM-DD` format |
| `file_name` | no (default `export.csv`) | name used for both the GCS object (`exports/<file_name>`) and the SFTP file |

### On a schedule (Cloud Scheduler)

```bash
gcloud scheduler jobs create http bq-sftp-daily \
  --location=us-central1 \
  --schedule="0 6 * * *" \
  --uri="${FUNCTION_URL}" \
  --http-method=POST \
  --message-body='{"dataset_table":"sales.daily_orders","file_name":"daily_orders_$(date +%Y%m%d).csv"}' \
  --oidc-service-account-email="${SA_EMAIL}" \
  --oidc-token-audience="${FUNCTION_URL}"
```

> Cloud Scheduler doesn't evaluate shell expressions in `--message-body` —
> for a date-partitioned filename, front this with a tiny Cloud Scheduler →
> Pub/Sub → Cloud Function trigger that builds the JSON payload, or template
> the date in a wrapper script that calls `gcloud scheduler jobs update`
> daily. For most teams, simpler to just let the function default to a
> fixed name and rely on GCS object versioning / SFTP overwrite for
> idempotency (see below).

---

## 4. Operational notes

- **Idempotency**: reruns with the same `file_name` overwrite both the GCS
  object and the SFTP file — safe to retry. If you need historical
  snapshots instead of overwrite, pass a date-partitioned `file_name` from
  the caller.
- **Large tables**: a single-file `EXTRACT` caps out around 1GB compressed.
  For bigger tables, change `destination_uri` in `main.py` to a wildcard
  (`gs://bucket/exports/file-*.csv`) and either concatenate the shards
  before the SFTP push, or loop the SFTP upload over each shard.
- **Host key verification**: `paramiko.Transport` trusts the SFTP host by
  default. For production, pin the host key — store its fingerprint
  alongside the SFTP secret and verify via
  `transport.get_remote_server_key()` before authenticating.
- **Observability**: only `logging` (→ Cloud Logging) is wired up today.
  For SLA tracking, add a custom Cloud Monitoring metric or a row to a
  reconciliation/audit table on success/failure.
- **Secret rotation**: add a new secret version (§1.1); no redeploy needed
  — the function always reads `versions/latest`, just restarts pick up
  the change (the `lru_cache` is per-instance, not persistent).

## Files

| File | Purpose |
|---|---|
| `main.py` | Function entry point and ordered orchestration |
| `config.py` | Secret Manager configuration |
| `source.py` | Loads and executes the BigQuery SQL |
| `target.py` | BigQuery export, GCS download, and SFTP upload |
| `queries/export_orders.sql` | The single query with joins, filters, and CASE expressions |
| `requirements.txt` | Python dependencies |
