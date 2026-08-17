"""GCS and SFTP target operations."""

import io
import logging
import posixpath

import paramiko
from google.cloud import bigquery, storage

logger = logging.getLogger(__name__)


def export_table_to_gcs(bq_project, table_ref, bucket, blob_path):
    destination_uri = f"gs://{bucket}/{blob_path}"
    job_config = bigquery.ExtractJobConfig(
        destination_format=bigquery.DestinationFormat.CSV,
        field_delimiter="|",
        print_header=True,
    )
    job = bigquery.Client(project=bq_project).extract_table(
        table_ref, destination_uri, job_config=job_config
    )
    job.result()
    logger.info("Exported query result to %s", destination_uri)
    return destination_uri


def download_gcs_blob(bucket_name, blob_path, local_path):
    storage.Client().bucket(bucket_name).blob(blob_path).download_to_filename(local_path)


def upload_to_sftp(cfg, local_path, remote_filename):
    transport = paramiko.Transport((cfg["sftp_host"], int(cfg.get("sftp_port", 22))))
    try:
        if cfg.get("sftp_private_key"):
            key = paramiko.RSAKey.from_private_key(io.StringIO(cfg["sftp_private_key"]))
            transport.connect(username=cfg["sftp_username"], pkey=key)
        else:
            transport.connect(
                username=cfg["sftp_username"], password=cfg["sftp_password"]
            )
        sftp = paramiko.SFTPClient.from_transport(transport)
        try:
            remote_path = posixpath.join(
                cfg.get("sftp_remote_path", "/"), remote_filename
            )
            sftp.put(local_path, remote_path)
            logger.info("Uploaded file to sftp://%s%s", cfg["sftp_host"], remote_path)
        finally:
            sftp.close()
    finally:
        transport.close()
