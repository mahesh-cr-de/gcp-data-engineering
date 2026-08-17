"""Runtime configuration loaded from one Secret Manager JSON secret."""

import json
import os
from functools import lru_cache

from google.cloud import secretmanager

CONFIG_SECRET_NAME = os.environ.get("CONFIG_SECRET_NAME", "bq-export-etl-config")
SECRET_PROJECT = os.environ.get("SECRET_PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT")


@lru_cache(maxsize=1)
def get_config() -> dict:
    if not SECRET_PROJECT:
        raise RuntimeError("Set GOOGLE_CLOUD_PROJECT or SECRET_PROJECT_ID")
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{SECRET_PROJECT}/secrets/{CONFIG_SECRET_NAME}/versions/latest"
    payload = client.access_secret_version(name=name).payload.data.decode("utf-8")
    return json.loads(payload)
